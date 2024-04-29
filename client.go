package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

type Client struct {
	ESURL    string
	User     string
	Password string
}

type ShardsMetaResult struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
	Failures   []struct {
		Shard  int    `json:"shard"`
		Index  string `json:"index"`
		Node   string `json:"node"`
		Reason struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"reason"`
	} `json:"failures"`
}

type SearchResult struct {
	ShardsMetaResult ShardsMetaResult `json:"_shards"`

	ScrollId string `json:"_scroll_id"`

	Hits struct {
		Total struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []json.RawMessage `json:"hits"`
	} `json:"hits"`
}

func (c *Client) Query(ctx context.Context, index string, query string, fetchAll bool, slices int, writer io.Writer) error {
	var docs atomic.Int64
	var totalDocs atomic.Int64
	if slices <= 1 {
		return c.querySlice(ctx, index, query, fetchAll, 0, 1, &docs, &totalDocs, nil, writer)
	}

	group, ctx := errgroup.WithContext(ctx)
	var writerLock sync.Mutex
	for i := 0; i < slices; i++ {
		group.Go(func() error {
			return c.querySlice(ctx, index, query, fetchAll, i, slices, &docs, &totalDocs, &writerLock, writer)
		})
	}
	return group.Wait()
}

func (c *Client) querySlice(ctx context.Context, index string, query string, fetchAll bool, slice int, maxSlices int, docs *atomic.Int64, totalDocs *atomic.Int64, writerLock *sync.Mutex, writer io.Writer) error {
	url := fmt.Sprintf("%s/_search?_source=true", index)
	if fetchAll {
		url += "&scroll=1m"
	}

	if maxSlices > 1 {
		var queryObj map[string]any
		if err := json.Unmarshal([]byte(query), &queryObj); err != nil {
			return fmt.Errorf("failed to parse query: %w", err)
		}
		queryObj["slice"] = map[string]int{"id": slice, "max": maxSlices}
		queryBytes, err := json.Marshal(queryObj)
		if err != nil {
			return fmt.Errorf("failed to re-marshal query with slice information: %w", err)
		}
		query = string(queryBytes)
	}

	_, data, err := c.do(ctx, "GET", url, query)
	if err != nil {
		return err
	}

	var sr SearchResult
	if err := json.Unmarshal(data, &sr); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if sr.ShardsMetaResult.Failed > 0 {
		return fmt.Errorf("failed to query Elasticsearch: %v", sr.ShardsMetaResult.Failures)
	}

	totalDocs.Add(sr.Hits.Total.Value)
	docs.Add(int64(len(sr.Hits.Hits)))

	if err := writeJsons(sr.Hits.Hits, writerLock, writer); err != nil {
		return err
	}

	if !fetchAll {
		return nil
	}

	return c.scroll(ctx, &sr, docs, totalDocs, writerLock, writer)
}

func (c *Client) scroll(ctx context.Context, sr *SearchResult, docs *atomic.Int64, totalDocs *atomic.Int64, writerLock *sync.Mutex, writer io.Writer) error {
	scrollId := sr.ScrollId
	defer func() {
		_, _, err := c.do(ctx, "DELETE", "_search/scroll", fmt.Sprintf(`{"scroll_id":"%s"}`, scrollId))
		if err != nil {
			log.Printf("failed to clear scroll: %v", err)
		}
	}()

	logProgress := func() {
		localDocs := docs.Load()
		localTotalDocs := totalDocs.Load()
		log.Printf("Fetched %d documents out of %d documents (%.1f%%)", localDocs, localTotalDocs, float64(localDocs)/float64(localTotalDocs)*100)
	}
	logProgress()

	for {
		body := fmt.Sprintf(`{"scroll":"1m","scroll_id":"%s"}`, scrollId)
		_, data, err := c.do(ctx, "POST", "_search/scroll", body)
		if err != nil {
			return err
		}

		var sr SearchResult
		if err := json.Unmarshal(data, &sr); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}

		if sr.ShardsMetaResult.Failed > 0 {
			return fmt.Errorf("failed to query Elasticsearch: %v", sr.ShardsMetaResult.Failures)
		}

		docs.Add(int64(len(sr.Hits.Hits)))
		logProgress()

		if err := writeJsons(sr.Hits.Hits, writerLock, writer); err != nil {
			return err
		}

		if len(sr.Hits.Hits) == 0 {
			break
		}

		scrollId = sr.ScrollId
	}

	return nil
}

func (c *Client) do(ctx context.Context, method string, path string, body string) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.pathURL(path), bytes.NewBufferString(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.User != "" {
		req.SetBasicAuth(c.User, c.Password)
	}

	// log.Printf("%s %s", req.Method, req.URL.String())
	// for k, v := range req.Header {
	// 	log.Printf("%s: %s", k, v)
	// }
	// log.Print()
	// log.Print(body)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query Elasticsearch: %w", err)
	}

	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response body: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		return nil, data, fmt.Errorf("failed to query Elasticsearch: %s", res.Status)
	}
	return res, data, nil
}

func (c *Client) pathURL(path string) string {
	url := c.ESURL
	for url[len(url)-1] == '/' {
		url = url[:len(url)-1]
	}
	for path != "" && path[0] == '/' {
		path = path[1:]
	}
	return fmt.Sprintf("%s/%s", c.ESURL, path)
}

func writeJsons(jsons []json.RawMessage, writerLock *sync.Mutex, writer io.Writer) error {
	writeEntry := func(entry json.RawMessage) error {
		if writerLock != nil {
			writerLock.Lock()
			defer writerLock.Unlock()
		}
		if _, err := writer.Write(entry); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
		if _, err := writer.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
		return nil
	}
	for _, entry := range jsons {
		if err := writeEntry(entry); err != nil {
			return err
		}
	}
	return nil
}
