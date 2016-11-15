package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"errors"
	"net/url"
	"encoding/json"
	"os"
	"time"
	"strings"
	"github.com/umbel/pilosa/pql"

//	"helpers"
)


// TODO: use go test
func test_client_schema(c *Client) {
	fmt.Printf("\nschema test\n")

	DBs, err := c.Schema()
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	fmt.Printf("database[0] name: %s\n", DBs[0].Name)
	fmt.Printf("frame[0] name: %s\n", DBs[0].Frames[0].Name)
}


func test_client_union(c *Client) {
	fmt.Printf("\nunion test\n")

	call := Union(Bitmap(20, "bar"), Bitmap(21, "bar"))

	result, err := c.Execute("ExampleDB", pql.Calls{call})
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	fmt.Printf("query: %s\n", call)
	fmt.Printf("result: %s\n", result)
}

func main() {
	c, err := NewClient(default_host)
	if err != nil {
		panic(err)
	}

	test_client_schema(c)
	test_client_union(c)

}

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
		host = default_host  // TODO: I prefer a useful default...
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

	if database == "" {
		return "", ErrDatabaseRequired
	}

	u := url.URL{
		Scheme: "http",
		Host: c.host,
		Path: "/query",
		RawQuery: url.Values{
			"db":    {database},
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

// convenience wrappers around pql types
func ClearBit(id uint64, frame string, profileID uint64) *pql.ClearBit {
	return &pql.ClearBit{
		ID: id,
		Frame: frame,
		ProfileID: profileID,
	}
}

func Count(bm pql.BitmapCall) *pql.Count {
	return &pql.Count{
		Input: bm,
	}
}

func Profile(id uint64) *pql.Profile {
	return &pql.Profile{
		ID: id,
	}
}

func SetBit(id uint64, frame string, profileID uint64) *pql.SetBit {
	return &pql.SetBit{
		ID: id,
		Frame: frame,
		ProfileID: profileID,
	}
}

func SetBitmapAttrs(id uint64, frame string, attrs map[string]interface{}) *pql.SetBitmapAttrs {
	return &pql.SetBitmapAttrs{
		ID: id,
		Frame: frame,
		Attrs: attrs,
	}
}

func SetProfileAttrs(id uint64, attrs map[string]interface{}) *pql.SetProfileAttrs {
	return &pql.SetProfileAttrs{
		ID: id,
		Attrs: attrs,
	}
}

func TopN(frame string, n int, src pql.BitmapCall, bmids []uint64, field string, filters []interface{}) *pql.TopN {
	return &pql.TopN{
		Frame: frame,
		N: n,
		Src: src,
		BitmapIDs: bmids,
		Field: field,
		Filters: filters,
	}
}

func Difference(bms ...pql.BitmapCall) *pql.Difference {
	// TODO does this need to be limited to two inputs?
	return &pql.Difference{
		Inputs: bms,
	}
}

func Intersect(bms ...pql.BitmapCall) *pql.Intersect {
	return &pql.Intersect{
		Inputs: bms,
	}
}

func Union(bms ...pql.BitmapCall) *pql.Union {
	return &pql.Union{
		Inputs: bms,
	}
}

func Bitmap(id uint64, frame string) *pql.Bitmap {
	return &pql.Bitmap{
		ID:    id,
		Frame: frame,
	}
}

func Range(id uint64, frame string, start time.Time, end time.Time) *pql.Range {
	return &pql.Range{
		ID:    id,
		Frame: frame,
		StartTime: start,
		EndTime: end,
	}
}
