package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/alexflint/go-arg"
)

type Args struct {
	ESURL       string `arg:"-u,--elasticsearch-url,required" help:"URL of the Elasticsearch cluster"`
	User        string `arg:"env:ES_USER" help:"User to authenticate with Elasticsearch"`
	Password    string `arg:"env:ES_PASSWD" help:"Password to authenticate with Elasticsearch"`
	Index       string `arg:"-i,--index,required" help:"Index to search in"`
	QueryString string `arg:"-q,--query" help:"Query to run against the index"`
	QueryFile   string `arg:"-f,--query-file" help:"File containing the query to run against the index"`
	FetchAll    bool   `arg:"-a,--fetch-all" help:"Fetch all results from the query by paginating through it. Use with caution, as this can be a lot of data. See also --slices"`
	Slices      int    `arg:"-s,--slices" default:"1" help:"Number of slices to use for the scroll query. Improves query performance. Only relevant if --fetch-all is passed. NOTE: Do not set a number of slices greater than the number of shards in the queried index"`
}

func (a Args) Query() (string, error) {
	if a.QueryFile != "" && a.QueryString != "" {
		return "", fmt.Errorf("both query and query-file were provided, please provide only one")
	}

	if a.QueryString != "" || a.QueryFile == "" {
		return a.QueryString, nil
	}

	file, err := os.Open(a.QueryFile)
	if err != nil {
		return "", fmt.Errorf("failed to read from file %s: %w", a.QueryFile, err)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("failed to read from file %s: %w", a.QueryFile, err)
	}
	return string(data), nil
}

func main() {
	var args Args
	arg.MustParse(&args)

	query, err := args.Query()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := Client{
		ESURL:    args.ESURL,
		User:     args.User,
		Password: args.Password,
	}

	if err := client.Query(ctx, args.Index, query, args.FetchAll, args.Slices, os.Stdout); err != nil {
		log.Fatal(err)
	}
}
