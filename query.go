package main

import (
	"fmt"
	"net/http"
	"os"
	"io/ioutil"
	"strings"
	"encoding/json"
)



var default_host = "127.0.0.1:15000"

type Client struct {
	host string
}

type Query interface {
	pql string
}

type Result interface {

}


// (map[string]interface{})
func (client *Client) send_query_string(db string, query string, profiles bool) ([]byte, error) {
	url := fmt.Sprintf("http://%s/query?db=%s", client.host, db)
	if profiles {
		url += "&profiles=true"
	}

	response, err := http.Post(
		url,
		"application/json",
		strings.NewReader(query),
	)

	if err != nil {
		fmt.Printf("%s", err)
		os.Exit(1)
	}
	defer response.Body.Close()

	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Printf("%s", err)
		os.Exit(1)
	}

	return contents, nil
}

func (client *Client) SetBit(id int, frame string, profile_id int) (Query) {
	pql := fmt.Sprintf("SetBit(id=%d, frame=\"%s\", profileID=%s", id, frame, profile_id)
	return Query(pql)
}


func main() {

	var client Client
	client.host = default_host
	resp, err := client.send_query_string("testdb", "Bitmap(id=10, frame='foo')", false)
	if err != nil {
		fmt.Printf("%s", err)
		os.Exit(1)
	}

	// resp := send_query_string_to_pilosa("127.0.0.1:15000", "testdb", "SetBit(id=10, frame='foo')", false)
	fmt.Println(string(resp))

	var objmap map[string]*json.RawMessage
	err = json.Unmarshal(resp, &objmap)

	if err != nil {
		fmt.Printf("%s", err)
		os.Exit(1)
	}

	fmt.Println(objmap)

}
