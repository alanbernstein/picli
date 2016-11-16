package pilosago

import (
	"fmt"
	"github.com/umbel/pilosa/pql"
	"os"
	"testing"
)

func TestClientSchema(t *testing.T) {
	c, err := NewClient(default_host)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("\nschema test\n")

	DBs, err := c.Schema()
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	fmt.Printf("database[0] name: %s\n", DBs[0].Name)
	fmt.Printf("frame[0] name: %s\n", DBs[0].Frames[0].Name)

	/*
		if a := f.Bitmap(0).Bits(); !reflect.DeepEqual(a, []uint64{1, 5}) {
			t.Fatalf("unexpected bits: %+v", a)
		}
		if a := f.Bitmap(200).Bits(); !reflect.DeepEqual(a, []uint64{6}) {
			t.Fatalf("unexpected bits: %+v", a)
		}
	*/

}

func TestClientUnion(t *testing.T) {
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
