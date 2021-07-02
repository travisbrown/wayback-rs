use fantoccini::error::NewSessionError;
use fantoccini::{Client, ClientBuilder};

// Adapted from the Fantoccini tests
pub(crate) async fn make_client(
    name: &str,
    headless: bool,
    host: Option<&str>,
    port: Option<u16>,
) -> Result<Client, NewSessionError> {
    match name {
        "firefox" => {
            let mut caps = serde_json::map::Map::new();
            let args = if headless {
                serde_json::json!(["--headless"])
            } else {
                serde_json::json!([])
            };
            let opts = { serde_json::json!({ "args": args }) };
            caps.insert("moz:firefoxOptions".to_string(), opts.clone());
            ClientBuilder::rustls()
                .capabilities(caps)
                .connect(&make_url(host, port.unwrap_or(4444)))
                .await
        }
        "chrome" => {
            let mut caps = serde_json::map::Map::new();
            let args = if headless {
                serde_json::json!([
                    "--headless",
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ])
            } else {
                serde_json::json!(["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"])
            };
            let opts = serde_json::json!({
                "args": args,
                "binary":
                    if std::path::Path::new("/usr/bin/chromium-browser").exists() {
                        // on Ubuntu, it's called chromium-browser
                        "/usr/bin/chromium-browser"
                    } else if std::path::Path::new("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome").exists() {
                        // macOS
                        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                    } else {
                        // elsewhere, it's just called chromium
                        "/usr/bin/chromium"
                    }
            });
            caps.insert("goog:chromeOptions".to_string(), opts.clone());

            ClientBuilder::rustls()
                .capabilities(caps)
                .connect(&make_url(host, port.unwrap_or(9515)))
                .await
        }
        browser => unimplemented!("unsupported browser backend {}", browser),
    }
}

fn make_url(host: Option<&str>, port: u16) -> String {
    format!("http://{}:{}", host.unwrap_or("localhost"), port)
}
