use chrono::NaiveDate;
use std::fs::File;
use std::io::{BufRead, BufReader, Error};
use wayback_rs::{cdx::IndexClient, Downloader, Item};

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

// These tests exercise the live Wayback Machine and can be flaky due to
// network conditions, service throttling, or transient server-side errors.
//
// Run explicitly with:
//   cargo test --test wayback_cdx -- --ignored
#[tokio::test]
#[ignore]
async fn test_search() {
    let client = IndexClient::default();
    let results = client.search(EXAMPLE_ITEM_QUERY, None, None).await.unwrap();

    assert_eq!(results[0], example_item());
}

#[tokio::test]
#[ignore]
async fn test_download() {
    let client = Downloader::default();
    let result = client.download_item(&example_item()).await.unwrap();
    let result_lines = result
        .lines()
        .collect::<Result<Vec<String>, Error>>()
        .unwrap();

    assert_eq!(result_lines, example_lines());
}
