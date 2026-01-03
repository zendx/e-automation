# e-automation

Async crawler that aggregates disposable/temporary email domains from multiple public sources and stores them in Postgres for downstream filtering.

## Features
- Pulls from built-in plain-text lists and HTML pages; supports custom sources via JSON.
- Async HTTP fetching with per-host limits, retries, response size caps, and ETag/Last-Modified caching.
- Optional DNS validation (MX/A) to keep only active domains.
- Persists domains and source metadata to Postgres for reuse across runs.

## Requirements
- Python 3.10+ (asyncio-based).
- Postgres database reachable via `DATABASE_URL`.
- Install dependencies: `pip install -r requirements.txt`.

## Configuration
Set environment variables before running:
- `DATABASE_URL` (required): Postgres connection string.
- `CONCURRENCY` (default 8): Max concurrent requests.
- `PER_HOST_LIMIT` (default 2): Semaphore per host to avoid hammering a source.
- `REQUEST_TIMEOUT` (default 10): Read timeout in seconds.
- `CONNECT_TIMEOUT` (default 5): Connection timeout in seconds.
- `MAX_RESPONSE_BYTES` (default 1048576): Hard cap on downloaded response size.
- `MAX_RETRIES` (default 3): Retry attempts for retryable HTTP failures.
- `USER_AGENT` (default set in code): Override crawler user agent.
- `SOURCES_JSON` (optional): Path to a JSON file defining extra sources (see below).
- `DNS_VALIDATE` (default true): Toggle DNS MX/A validation of discovered domains.
- `DNS_TIMEOUT` (default 3): Timeout per DNS lookup in seconds.
- `DNS_CONCURRENCY` (default 20): Max concurrent DNS lookups.

## Running the crawler
```sh
# Example
set DATABASE_URL=postgresql://user:pass@host:5432/dbname
python -m src.crawler
```
The script initializes the schema (tables `sources` and `domains`), upserts source definitions, fetches domains, optionally validates them via DNS, and upserts results.

## Adding custom sources
Provide `SOURCES_JSON` pointing to a file shaped like:
```json
[
  {
    "name": "example_list",
    "url": "https://example.com/domains.txt",
    "kind": "list"
  },
  {
    "name": "example_html",
    "url": "https://example.com/domains",
    "kind": "html",
    "selector": "a.domain-link",
    "attribute": "href"
  }
]
```
- `kind`: `list` for newline-delimited text, `html` to scrape with a CSS selector.
- `selector`: CSS selector to find nodes (used when `kind` is `html`).
- `attribute`: Optional attribute to pull from matching nodes (e.g., `href`); otherwise text content is inspected.

## Data model
- `sources`: Tracks source URL, ETag, Last-Modified, and last crawl time.
- `domains`: Primary-keyed by domain; stores first_seen/last_seen timestamps, last source, status, and notes.

## Tips
- Reruns reuse cached ETag/Last-Modified headers to skip unchanged sources.
- Set `DNS_VALIDATE=false` to skip DNS checks when you just want raw domain lists.
