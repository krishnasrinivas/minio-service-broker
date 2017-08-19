package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/minio/minio-go/pkg/s3utils"
	"github.com/minio/minio-service-broker/utils"
)

type ApiClient struct {
	log         lager.Logger
	httpClient  *http.Client
	conf        utils.Config
	endpointURL url.URL
}

// requestMetadata - is container for all the values to make a request.
type requestMetadata struct {

	// User supplied.
	instanceID   string
	bindingID    string
	queryValues  url.Values
	customHeader http.Header
	expires      int64

	// Generated by our internal code.

	contentBody   io.Reader
	contentLength int64
}

func New(config utils.Config, logger lager.Logger) (*ApiClient, error) {
	endpointURL, err := utils.GetEndpointURL(config.Endpoint, config.Secure)
	if err != nil {
		logger.Fatal("Could not construct agent endpoint", err)
	}
	defaultTransport := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := ApiClient{
		conf:        config,
		log:         logger,
		endpointURL: *endpointURL,
		httpClient: &http.Client{
			Transport: defaultTransport,
		},
	}

	fmt.Println("creating client to talk to service agent......")

	return &client, nil
}
func (c *ApiClient) CreateInstance(parameters map[string]string) (string, error) {
	// PUT server instance create metadata.
	reqMetadata := requestMetadata{
		instanceID: parameters["instanceID"],
	}
	fmt.Println("inside createinstance<== client call to agent ")
	// Execute PUT to create a new bucket.
	resp, err := c.executeMethod("PUT", reqMetadata)
	defer closeResponse(resp)
	if err != nil {
		c.log.Error("service agent returned error while provisioning", err)
		return "", err
	}
	responseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	return string(responseData), nil
}
func (c *ApiClient) GetInstanceState(instanceID string) (utils.Credentials, error) {
	// Get server binding create metadata.
	reqMetadata := requestMetadata{
		instanceID: instanceID,
	}
	fmt.Println("inside GetInstanceState<== client call to agent ")
	// Execute PUT to create a new bucket.
	resp, err := c.executeMethod("GET", reqMetadata)
	defer closeResponse(resp)
	if err != nil {
		c.log.Error("service agent could not retrieve instance credentials", err)
		return utils.Credentials{}, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.log.Fatal("Could not read response body", err)
	}

	creds, err := getCredentialsJSONResponse([]byte(body))
	return *creds, err
}

func getCredentialsJSONResponse(body []byte) (*utils.Credentials, error) {
	var c = new(utils.Credentials)
	err := json.Unmarshal(body, &c)
	if err != nil {
		fmt.Println("credentials unmarshalling Error:", err)
	}
	return c, err
}
func (c *ApiClient) DeleteInstance(instanceID string) error {
	// DELETE server instance create metadata.
	reqMetadata := requestMetadata{
		instanceID: instanceID,
	}
	// Execute PUT to create a new bucket.
	resp, err := c.executeMethod("DELETE", reqMetadata)
	defer closeResponse(resp)
	if err != nil {
		c.log.Error("service agent returned error while deprovisioning", err)
		return err
	}

	return nil
}

// Convert string to bool and always return false if any error
func mustParseBool(str string) bool {
	b, err := strconv.ParseBool(str)
	if err != nil {
		return false
	}
	return b
}

// do - execute http request.
func (c ApiClient) do(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error
	// Do the request in a loop in case of 307 http is met since golang still doesn't
	// handle properly this situation (https://github.com/golang/go/issues/7912)
	for {
		resp, err = c.httpClient.Do(req)
		if err != nil {
			// Handle this specifically for now until future Golang
			// versions fix this issue properly.
			urlErr, ok := err.(*url.Error)
			if ok && strings.Contains(urlErr.Err.Error(), "EOF") {
				return nil, &url.Error{
					Op:  urlErr.Op,
					URL: urlErr.URL,
					Err: fmt.Errorf("Connection closed by foreign host %s", urlErr.URL),
				}
			}
			return nil, err
		}
		// Redo the request with the new redirect url if http 307 is returned, quit the loop otherwise
		if resp != nil && resp.StatusCode == http.StatusTemporaryRedirect {
			newURL, uErr := url.Parse(resp.Header.Get("Location"))
			if uErr != nil {
				break
			}
			req.URL = newURL
		} else {
			break
		}
	}

	// Response cannot be non-nil, report if its the case.
	if resp == nil {
		msg := "Response is empty. " // + reportIssue
		return nil, errors.New(msg)
	}
	return resp, nil
}

// List of success status.
var successStatus = []int{
	http.StatusOK,
	http.StatusNoContent,
	http.StatusPartialContent,
}

// executeMethod - instantiates a given method, and retries the
// request upon any error up to maxRetries attempts in a binomially
// delayed manner using a standard back off algorithm.
func (c ApiClient) executeMethod(method string, reqData requestMetadata) (res *http.Response, err error) {

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{}, 1)

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	// Instantiate a new request.
	var req *http.Request
	req, err = c.newRequest(method, reqData)
	if err != nil {
		return nil, err
	}

	// Initiate the request.
	res, err = c.do(req)
	if err != nil {
		return nil, err
	}

	// For any known successful http status, return quickly.
	for _, httpStatus := range successStatus {
		if httpStatus == res.StatusCode {
			return res, nil
		}
	}

	// Read the body to be saved later.
	errBodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	// Save the body.
	errBodySeeker := bytes.NewReader(errBodyBytes)
	res.Body = ioutil.NopCloser(errBodySeeker)

	// Save the body back again.
	errBodySeeker.Seek(0, 0) // Seek back to starting point.
	res.Body = ioutil.NopCloser(errBodySeeker)

	return res, err
}

// newRequest - instantiate a new HTTP request for a given method.
func (c ApiClient) newRequest(method string, reqData requestMetadata) (req *http.Request, err error) {
	// If no method is supplied default to 'POST'.
	if method == "" {
		method = "POST"
	}

	// Construct a new target URL.
	targetURL, err := c.makeTargetURL(reqData.instanceID, reqData.bindingID, reqData.queryValues)
	fmt.Println("targetURL===", targetURL)
	if err != nil {
		return nil, err
	}

	// Initialize a new HTTP request for the method.
	req, err = http.NewRequest(method, targetURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// Set content body if available.
	if reqData.contentBody != nil {
		req.Body = ioutil.NopCloser(reqData.contentBody)
	}

	// set incoming content-length.
	if reqData.contentLength > 0 {
		req.ContentLength = reqData.contentLength
	}

	// Return request.
	return req, nil
}

// makeTargetURL make a new target url.
func (c ApiClient) makeTargetURL(instanceID string, bindingID string, queryValues url.Values) (*url.URL, error) {

	host := c.endpointURL.Host
	scheme := c.endpointURL.Scheme

	urlStr := scheme + "://" + host + "/"

	if instanceID != "" {
		urlStr = urlStr + s3utils.EncodePath("instances/"+instanceID)
	}
	if bindingID != "" {
		urlStr = urlStr + s3utils.EncodePath("bindings/"+bindingID)
	}
	// If there are any query values, add them to the end.
	if len(queryValues) > 0 {
		urlStr = urlStr + "?" + s3utils.QueryEncode(queryValues)
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	return u, nil
}

// closeResponse close non nil response with any response Body.

// convenient wrapper to drain any remaining data on response body.
//
// Subsequently this allows golang http RoundTripper
// to re-use the same connection for future requests.
func closeResponse(resp *http.Response) {
	// Callers should close resp.Body when done reading from it.
	// If resp.Body is not closed, the Client's underlying RoundTripper
	// (typically Transport) may not be able to re-use a persistent TCP
	// connection to the server for a subsequent "keep-alive" request.
	if resp != nil && resp.Body != nil {
		// Drain any remaining Body and then close the connection.
		// Without this closing connection would disallow re-using
		// the same connection for future uses.
		//  - http://stackoverflow.com/a/17961593/4465767
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
}
