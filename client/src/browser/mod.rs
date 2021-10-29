use async_std::task::sleep;
use fantoccini::{error::CmdError, Client as FClient, Locator};
use std::time::Duration;

pub struct Client {
    underlying: FClient,
}

impl Client {
    const LOGIN_URL: &'static str = "https://archive.org/account/login";
    const SAVE_URL: &'static str = "https://web.archive.org/save";
    const LOGIN_FORM_LOC: Locator<'static> = Locator::Css("form[name='login-form']");
    const SAVE_FORM_LOC: Locator<'static> = Locator::Css("#web-save-form");
    const SAVE_DONE_LOC: Locator<'static> = Locator::XPath(
        "//div[@id='spn-result']/span/a[contains(@href, '/web/')] | //div[@id='spn-result']/p[@class='text-danger']"
    );
    const SAVE_WAIT_MILLIS: u64 = 1000;

    pub fn new(client: FClient) -> Client {
        Client { underlying: client }
    }

    pub async fn login(&mut self, username: &str, password: &str) -> Result<(), CmdError> {
        self.underlying.goto(Self::LOGIN_URL).await?;
        let mut form = self.underlying.form(Self::LOGIN_FORM_LOC).await?;
        form.set_by_name("username", username)
            .await?
            .set_by_name("password", password)
            .await?
            .submit()
            .await?;

        Ok(())
    }

    pub async fn save<'a>(&'a mut self, url: &'a str) -> Result<Option<String>, CmdError> {
        sleep(Duration::from_millis(Self::SAVE_WAIT_MILLIS)).await;
        self.underlying.goto(Self::SAVE_URL).await?;

        self.underlying
            .wait()
            .forever()
            .for_element(Self::SAVE_FORM_LOC)
            .await?;
        let mut form = self.underlying.form(Self::SAVE_FORM_LOC).await?;
        form.set_by_name("url", url)
            .await?
            .set_by_name("capture_screenshot", "on")
            .await?
            .set_by_name("wm-save-mywebarchive", "on")
            .await?
            .set_by_name("email_result", "on")
            .await?
            .submit()
            .await?;

        let mut result = self
            .underlying
            .wait()
            .forever()
            .for_element(Self::SAVE_DONE_LOC)
            .await?;
        let result_href = result.attr("href").await?;

        Ok(result_href)
    }
}
