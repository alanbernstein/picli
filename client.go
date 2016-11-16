package pilosago

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/umbel/pilosa/pql"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

var default_host = "127.0.0.1:15000"

// TODO: move common Errs, types, etc into some parent package
var (
	// ErrHostRequired is returned when excuting a remote operation without a host.
	ErrHostRequired = errors.New("host required")

	// ErrDatabaseRequired is returned when no database is specified.
	ErrDatabaseRequired = errors.New("database required")

	// ErrFrameRequired is returned when no frame is specified.
	ErrFrameRequired = errors.New("frame required")

	// ErrFragmentNotFound is returned when a fragment does not exist.
	ErrFragmentNotFound = errors.New("fragment not found")

	// ErrQueryRequired is returned when no query is specified.
	ErrQueryRequired = errors.New("query required")

	// ErrBitRequired is returned when no bit is specified.
	ErrBitRequired = errors.New("bit required")
)

type DBInfo struct {
	Name   string       `json:"name"`
	Frames []*FrameInfo `json:"frames"`
}

type getSchemaResponse struct {
	DBs []*DBInfo `json:"dbs"`
}

type FrameInfo struct {
	Name string `json:"name"`
}

// Client represents a client to the Pilosa cluster.
type Client struct {
	host string

	// The client to use for HTTP communication.
	// Defaults to the http.DefaultClient.
	HTTPClient *http.Client
}

type Result struct {
	// TODO see internal.pb.go
}

// NewClient returns a new instance of Client to connect to host.
func NewClient(host string) (*Client, error) {
	// TODO use default database
	if host == "" {
		//return nil, ErrHostRequired
		host = default_host // TODO: I prefer a useful default...
	}

	return &Client{
		host:       host,
		HTTPClient: http.DefaultClient,
	}, nil
}

// Schema returns all database and frame schema information.
func (c *Client) Schema() ([]*DBInfo, error) {
	// Execute request against the host.
	u := url.URL{
		Scheme: "http",
		Host:   c.host,
		Path:   "/schema",
	}
	resp, err := c.HTTPClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rsp getSchemaResponse
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return rsp.DBs, nil
}

func (c *Client) Execute(database string, calls pql.Calls) (string, error) {
	// TODO accept variadic Call rather than Calls (?)
	// TODO protobuf stuff (see pilosa/client.go)
	// TODO parse http response into QueryResponse, containing list of QueryResult

	// TODO handle profiles argument (?) should that be a different function?
	// if profiles:
	// 	url += "&profiles=true"

	if database == "" {
		return "", ErrDatabaseRequired
	}

	u := url.URL{
		Scheme: "http",
		Host:   c.host,
		Path:   "/query",
		RawQuery: url.Values{
			"db": {database},
		}.Encode(),
	}

	q := pql.Query{Calls: calls}

	resp, err := c.HTTPClient.Post(
		u.String(),
		"application/octet-stream",
		strings.NewReader(q.String()),
	)

	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)

	return string(contents), nil
}
