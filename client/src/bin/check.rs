type Void = Result<(), Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Void {
    let args: Vec<String> = std::env::args().collect();

    use std::io::{self, BufRead};
    let file = std::fs::File::open(args.get(1).unwrap())?;

    let client = wayback_client::cdx::IndexClient::default();
    let mut writer = csv::Writer::from_writer(std::io::stdout());

    for result in io::BufReader::new(file).lines() {
        let line = result?;
        let mut fields = line.split(',');
        let digest = fields.next().unwrap();
        let url = fields.next().unwrap();

        if let Ok(items) = client.search(url, None, Some(digest)).await {
            //let items = client.search(url, None, None).await?;

            if items.is_empty() {
                println!("{},{},", digest, url);
            }

            for item in items {
                writer.write_record(item.to_record())?;
                writer.flush()?;
            }
        }
    }

    Ok(())
}
