pub fn guess_redirect_content(url: &str) -> String {
    format!(
        "<html><body>You are being <a href=\"{}\">redirected</a>.</body></html>",
        url
    )
}
