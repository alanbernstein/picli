package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"errors"
	"net/url"
	"encoding/json"
	"os"
	"strings"

	"github.com/umbel/pilosa/pql"
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

// Bit represents the location of a single bit.
type Bit struct {
	BitmapID  uint64
	ProfileID uint64
}

// Bits represents a slice of bits.
type Bits []Bit


type Profile struct {
	ID    uint64                 `json:"id"`
	Attrs map[string]interface{} `json:"attrs,omitempty"`
}

// Client represents a client to the Pilosa cluster.
type Client struct {
	host string

	// The client to use for HTTP communication.
	// Defaults to the http.DefaultClient.
	HTTPClient *http.Client
}

type Query struct {
	Name        string
	ID          int
	Frame       string
	ProfileID   int
	Inputs      []Query
	//Attributes  []string  // FIXME
	// what about start, end, n?
}

type Result struct {
	// TODO
}


func (q Query) String() string {
	// TODO recursive:
 	// subq.String() for subq in q.Inputs

	// FIXME
	switch q.Name {
	case "SetBit":
		return fmt.Sprintf("SetBit(%d, \"%s\", %d)", q.ID, q.Frame, q.ProfileID)
	case "ClearBit":
		return fmt.Sprintf("ClearBit(%d, \"%s\", %d)", q.ID, q.Frame, q.ProfileID)
	default:
		return "Query()"
	}
}


// NewClient returns a new instance of Client to connect to host.
func NewClient(host string) (*Client, error) {
	if host == "" {
		//return nil, ErrHostRequired
		host = default_host
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

func (c *Client) Execute(database string, query Query) (string, error) {  // FIXME return "result"
	// curl -X POST "http://127.0.0.1:15000/query?db=exampleDB" -d "SetBit(id=10, frame="foo", profileID=1)"
	if database == "" {
		return "", ErrDatabaseRequired
	} /* else if query == nil {
		return "", ErrQueryRequired
	} */

	u := url.URL{
		Scheme: "http",
		Host: c.host,
		Path: "/query",
		RawQuery: url.Values{
			"db":    {database},
		}.Encode(),
	}

	resp, err := c.HTTPClient.Post(u.String(), "application/octet-stream", strings.NewReader(query.String()))

	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)

	return string(contents), nil
}

// TODO error handling (later)
// TODO make it better
func SetBit(id int, frame string, profileID int) (Query) {
	return Query{
		Name: "SetBit",
		ID: id,
		Frame: frame,
		ProfileID: profileID,
	}
}

func ClearBit(id int, frame string, profileID int) (Query) {
	return Query{
		Name: "ClearBit",
		ID: id,
		Frame: frame,
		ProfileID: profileID,
	}
}

func Bitmap(id int, frame string) (Query) {
	return Query{
		Name: "Bitmap",
		ID: id,
		Frame: frame,
	}
}

/*
  func SetBitmapAttrs() (Query) {
  func SetProfileAttrs() (Query) {
  func Union() (Query) {
  func Intersect() (Query) {
  func Difference() (Query) {
  func Count() (Query) {
  func Range() (Query) {
  func TopN() (Query) {
*/

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

func test_client_setbit(c *Client) {
	fmt.Printf("\nsetbit test\n")

	Q := SetBit(20, "bar", 1)
	result, err := c.Execute("ExampleDB", Q)

	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	fmt.Printf("%s\n", result)

}

func main() {
	c, err := NewClient(default_host)
	if err != nil {
		panic(err)
	}

	test_client_schema(c)
	test_client_setbit(c)
}
