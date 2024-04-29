# esfetcher

Simple golang application to fetch documents from Elasticsearch. Supports pagination

Output is a series of json objects, one per line

```
% go run . --help
Program to fetch documents from Elasticsearch. Supports pagination

Usage: esfetch --elasticsearch-url ELASTICSEARCH-URL [--user USER] [--password PASSWORD] --index INDEX [--query QUERY] [--query-file QUERY-FILE] [--fetch-all] [--slices SLICES]

Options:
  --elasticsearch-url ELASTICSEARCH-URL, -u ELASTICSEARCH-URL
                         URL of the Elasticsearch cluster
  --user USER            Basic Auth User to authenticate with Elasticsearch [env: ES_USER]
  --password PASSWORD    Basic Auth Password to authenticate with Elasticsearch [env: ES_PASSWD]
  --index INDEX, -i INDEX
                         Index to search in
  --query QUERY, -q QUERY
                         Query to run against the index
  --query-file QUERY-FILE, -f QUERY-FILE
                         File containing the query to run against the index
  --fetch-all, -a        Fetch all results from the query by paginating through it. Use with caution, as this can be a lot of data. See also --slices
  --slices SLICES, -s SLICES
                         Number of slices to use for the scroll query. Improves fetching performance by running queries in parallel. Only relevant if --fetch-all is passed. NOTE: Do not set a number of slices greater than the number of shards in the queried index. See more at https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html [default: 1]
  --help, -h             display this help and exit
```

## Examples

```
# Simple run to fetch 10 documents from the index
% go run . --elasticsearch-url https://some.elasticsearch.service.com:9200 --index 'my-index' --query '{"size": 10}'
{ "_index": "my-index", "_id": "cxzN144BCRyX4VLEIPJZ", "_score": 0.0, "_source": { "@timestamp": "2024-04-13T14:12:07.214369100Z", "some_key": "some_value", ... } }
...


# Fetch all documents (with pagination) for a more complex query saved as `query.json`
% cat query.json
{
  "size": 10000,
  "query": {
    "bool": {
      "filter": [
        {
          "match": {
            "my_attribute": "some_value"
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": "2024-04-08T00:00:00Z",
              "lt": "2024-04-15T00:00:00Z"
            }
          }
        }
      ]
    }
  }
}

% go run . --elasticsearch-url https://some.elasticsearch.service.com:9200 --index 'my-index' --query-file query.json --fetch-all --slices 10
{ "_index": "my-index", "_id": "cxzN144BCRyX4VLEIPJZ", "_score": 0.0, "_source": { "@timestamp": "2024-04-13T14:12:07.214369100Z", "some_key": "some_value", ... } }
...

```