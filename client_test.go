package pilosago

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/umbel/pilosa/pql"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	//	"helpers"
)

func test_client_schema() {
	c, err := NewClient(default_host)
	if err != nil {
		panic(err)
	}

	fmt.Printf("\nschema test\n")

	DBs, err := c.Schema()
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	fmt.Printf("database[0] name: %s\n", DBs[0].Name)
	fmt.Printf("frame[0] name: %s\n", DBs[0].Frames[0].Name)
}

func test_client_union() {
	c, err := NewClient(default_host)
	if err != nil {
		panic(err)
	}

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
