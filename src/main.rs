use chrono::{DateTime, Datelike, TimeZone, Utc};
use hyperliquid_rust_sdk::{BaseUrl, InfoClient};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Clone, Serialize, Deserialize)]
struct FundingHistoryResponse {
    coin: String,
    funding_rate: f32,
    premium: f32,
    timestamp: DateTime<Utc>,
}

impl From<&hyperliquid_rust_sdk::FundingHistoryResponse> for FundingHistoryResponse {
    fn from(value: &hyperliquid_rust_sdk::FundingHistoryResponse) -> Self {
        Self {
            coin: value.coin.clone(),
            funding_rate: value.funding_rate.parse().unwrap(),
            premium: value.premium.parse().unwrap(),
            timestamp: DateTime::from_timestamp_millis(value.time as i64).unwrap(),
        }
    }
}

#[derive(Deserialize)]
struct AssetsConfig {
    assets: Vec<String>,
}

fn read_assets_from_json(file_path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(file_path)?;
    let config: AssetsConfig = serde_json::from_str(&content)?;
    Ok(config.assets)
}

const EXPECTED_ENDPOINT_TOTAL: usize = 500;

#[tokio::main]
async fn main() {
    let info_client = InfoClient::new(None, Some(BaseUrl::Mainnet)).await.unwrap();

    let assets = read_assets_from_json("./config.json").unwrap();

    for asset in assets {
        println!("Getting funding history for: {}", asset);

        let mut csv = csv::Writer::from_path(format!("./{}.csv", asset)).unwrap();

        let mut from = Utc
            .with_ymd_and_hms(Utc::now().year(), 1, 1, 0, 0, 0)
            .unwrap();

        println!("Start: {:?}", from.to_rfc3339());

        loop {
            let history = info_client
                .funding_history(asset.to_string(), from.timestamp_millis() as u64, None)
                .await
                .unwrap()
                .iter()
                .map(FundingHistoryResponse::from)
                .collect::<Vec<_>>();

            from = history.last().unwrap().timestamp;

            history.iter().for_each(|h| {
                csv.serialize(h).unwrap();
            });
            csv.flush().unwrap();

            if history.len() < EXPECTED_ENDPOINT_TOTAL {
                break;
            }
        }

        println!("End: {:?}", from.to_rfc3339());
    }
}
