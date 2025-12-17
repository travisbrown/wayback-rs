use chrono::NaiveDate;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::fs::File;
use std::io::{BufRead, BufReader, Error};
use std::sync::Arc;
use std::time::Duration;
use wayback_rs::{cdx::IndexClient, Downloader, Item, Pacer};

const EXAMPLE_ITEM_QUERY: &str = "twitter.com/travisbrown/status/1323554460765925376";

fn example_item() -> Item {
    Item::new(
        format!("https://{}", EXAMPLE_ITEM_QUERY),
        NaiveDate::from_ymd_opt(2020, 11, 3)
            .and_then(|date| date.and_hms_opt(9, 16, 10))
            .unwrap(),
        "BHEPEG22C5COEOQD46QEFH4XK5SLN32A".to_string(),
        "text/html".to_string(),
        2948,
        Some(200),
    )
}

fn example_lines() -> Vec<String> {
    let file = File::open("examples/html/1323554460765925376.html").unwrap();
    let reader = BufReader::new(file);

    reader
        .lines()
        .collect::<Result<Vec<String>, Error>>()
        .unwrap()
}

fn default_test_pacer() -> Arc<Pacer> {
    // Conservative defaults for live Wayback testing:
    // - CDX: ~1 req/sec
    // - Content: ~1 req / 1500ms
    Arc::new(Pacer::new(
        || async {
            tokio::time::sleep(Duration::from_secs(1)).await;
        },
        || async {
            tokio::time::sleep(Duration::from_millis(1500)).await;
        },
    ))
}

async fn log_raw_cdx_response_preview(query: &str) {
    // Mirror the URL construction in wayback_rs::cdx for the default client.
    // Note: wayback-rs uses HTTP (not HTTPS) for the CDX endpoint by default.
    let url = format!(
        "http://web.archive.org/cdx/search/cdx?url={}&output=json&fl=original,timestamp,digest,mimetype,length,statuscode",
        query
    );

    async fn do_request(label: &str, url: &str, headers: Option<HeaderMap>) {
        let client = reqwest::Client::new();
        let builder = client.get(url);
        let builder = if let Some(h) = headers {
            builder.headers(h)
        } else {
            builder
        };

        let req = match builder.build() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{label}: request build failed: {e:?}");
                return;
            }
        };

        eprintln!("--- CDX RAW REQUEST ({label}) ---");
        eprintln!("request_line: GET {} HTTP/1.1", req.url());
        for (k, v) in req.headers().iter() {
            eprintln!("{}: {}", k.as_str(), v.to_str().unwrap_or("<non-utf8>"));
        }
        eprintln!("--- END CDX RAW REQUEST ({label}) ---");

        let resp = match client.execute(req).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{label}: request failed: {e:?}");
                return;
            }
        };

        let status = resp.status();
        let headers: HeaderMap = resp.headers().clone();
        let body = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("{label}: body read failed: {e:?}");
                return;
            }
        };

        eprintln!("--- CDX RAW RESPONSE PREVIEW ({label}) ---");
        eprintln!("url: {url}");
        eprintln!("status: {status}");
        for key in [
            "content-type",
            "server",
            "date",
            "x-app-server",
            "x-location",
            "x-rl",
            "x-na",
            "x-ts",
            "x-tr",
            "server-timing",
            "via",
            "cf-ray",
            "cf-cache-status",
        ] {
            if let Some(v) = headers.get(key) {
                eprintln!("{key}: {}", v.to_str().unwrap_or("<non-utf8>"));
            }
        }
        let preview_len = body.len().min(600);
        let preview = String::from_utf8_lossy(&body[..preview_len]);
        eprintln!("body_len: {}", body.len());
        eprintln!("body_preview:\n{preview}");
        eprintln!("--- END CDX RAW RESPONSE PREVIEW ({label}) ---");
    }

    // Variant A: explicit, stable headers for diagnostics.
    let mut diag_headers = HeaderMap::new();
    diag_headers.insert(
        HeaderName::from_static("user-agent"),
        HeaderValue::from_static("wayback-rs-test (reqwest)"),
    );
    diag_headers.insert(
        HeaderName::from_static("accept"),
        HeaderValue::from_static("application/json"),
    );

    // Variant B: no explicit headers (closer to the library's defaults).
    do_request("headers=explicit", &url, Some(diag_headers)).await;
    do_request("headers=default", &url, None).await;

    // Note: the integration test still uses `IndexClient` for the actual
    // assertion; this helper is purely for diagnostics when that path fails.
}

// These tests exercise the live Wayback Machine and can be flaky due to
// network conditions, service throttling, or transient server-side errors.
//
// Run explicitly with:
//   cargo test --test wayback_cdx -- --ignored
#[tokio::test]
#[ignore]
async fn test_search() {
    let client = IndexClient::default().with_pacer(default_test_pacer());

    // `IndexClient::search` does not currently go through wayback-rs retry logic.
    // Since this is a live-network test, add a small bounded retry for the
    // transient failure modes we see in practice (empty / non-JSON bodies).
    let mut last_error = None;
    let mut results = None;

    for attempt in 0..3 {
        match client.search(EXAMPLE_ITEM_QUERY, None, None).await {
            Ok(v) => {
                results = Some(v);
                break;
            }
            Err(e @ wayback_rs::cdx::Error::HttpClientError(_))
            | Err(e @ wayback_rs::cdx::Error::JsonError(_)) => {
                // Best-effort diagnostics: print what the service actually returned.
                // This can help detect rate limiting, transient errors, or unexpected
                // content (HTML, empty body, etc.).
                log_raw_cdx_response_preview(EXAMPLE_ITEM_QUERY).await;
                last_error = Some(e);
                tokio::time::sleep(Duration::from_secs(2 + attempt)).await;
            }
            Err(other) => panic!("Unexpected CDX error: {:?}", other),
        }
    }

    let results = results.unwrap_or_else(|| panic!("CDX search failed: {:?}", last_error));

    assert_eq!(results[0], example_item());
}

#[tokio::test]
#[ignore]
async fn test_download() {
    let client = Downloader::default().with_pacer(default_test_pacer());
    let result = client.download_item(&example_item()).await.unwrap();
    let result_lines = result
        .lines()
        .collect::<Result<Vec<String>, Error>>()
        .unwrap();

    assert_eq!(result_lines, example_lines());
}
