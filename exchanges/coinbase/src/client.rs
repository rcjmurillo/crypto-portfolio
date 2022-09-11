use std::{env, fmt, marker::PhantomData};

use anyhow::{Error, Result};
use base64;
use bytes::Bytes;
use chrono::{Duration, NaiveDate, Utc};
use futures::future::join_all;
use hex::encode as hex_encode;
use hmac::{Hmac, Mac, NewMac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::de::DeserializeOwned;
use sha2::Sha256;

use api_client::{errors::Error as ApiError, ApiClient, Query};

use crate::api_model::{Account, Fill, Product, Response, Transaction};
use market::Market;

pub trait Identifiable<T> {
    fn id(&self) -> &T;
}

pub struct Std;
pub struct Pro;

enum ApiEndpoint {
    Std,
    Pro,
}

impl Into<&str> for &ApiEndpoint {
    fn into(self) -> &'static str {
        match self {
            ApiEndpoint::Std => "https://api.coinbase.com",
            ApiEndpoint::Pro => "https://api.pro.coinbase.com",
        }
    }
}

impl Into<&str> for ApiEndpoint {
    fn into(self) -> &'static str {
        (&self).into()
    }
}

impl fmt::Display for ApiEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Into::<&str>::into(self) as &str)
    }
}

enum StdEndpoints {
    Accounts,
    Transactions,
    Deposits,
    Withdraws,
    Buys,
    Sells,
}

impl Into<&'static str> for &StdEndpoints {
    fn into(self) -> &'static str {
        match self {
            StdEndpoints::Accounts => "/v2/accounts",
            StdEndpoints::Transactions => "/v2/accounts/{account_id}/transactions",
            StdEndpoints::Deposits => "/v2/accounts/{account_id}/deposits",
            StdEndpoints::Withdraws => "/v2/accounts/{account_id}/withdraws",
            StdEndpoints::Buys => "/v2/accounts/{account_id}/buys",
            StdEndpoints::Sells => "/v2/accounts/{account_id}/sells",
        }
    }
}

impl Into<&str> for StdEndpoints {
    fn into(self) -> &'static str {
        (&self).into()
    }
}

impl fmt::Display for StdEndpoints {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Into::<&str>::into(self) as &str)
    }
}

enum ProEndpoints {
    Fills,
    Products,
}

impl Into<&'static str> for &ProEndpoints {
    fn into(self) -> &'static str {
        match self {
            ProEndpoints::Fills => "/fills",
            ProEndpoints::Products => "/products",
        }
    }
}

impl Into<&str> for ProEndpoints {
    fn into(self) -> &'static str {
        (&self).into()
    }
}

impl fmt::Display for ProEndpoints {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Into::<&str>::into(self) as &str)
    }
}

enum Endpoints {
    Std(StdEndpoints),
    Pro(ProEndpoints),
}

struct Credentials<T> {
    key: String,
    secret: String,
    passphrase: Option<String>,
    api: PhantomData<T>,
}

impl Credentials<Std> {
    fn new() -> Self {
        Self {
            key: env::var("COINBASE_STD_API_KEY").expect("missing coinbase standard API token"),
            secret: env::var("COINBASE_STD_API_SECRET")
                .expect("missing coinbase standard API token"),
            passphrase: None,
            api: PhantomData,
        }
    }
}

impl Credentials<Pro> {
    fn new() -> Self {
        Self {
            key: env::var("COINBASE_PRO_API_KEY").expect("missing coinbase pro API token"),
            secret: env::var("COINBASE_PRO_API_SECRET").expect("missing coinbase pro API token"),
            passphrase: Some(
                env::var("COINBASE_PRO_API_PASSPHRASE")
                    .expect("missing coinbase pro API passphrase"),
            ),
            api: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub start_date: NaiveDate,
    pub symbols: Vec<String>,
}

pub struct CoinbaseFetcher<Api> {
    api_client: ApiClient,
    credentials: Credentials<Api>,
    api: PhantomData<Api>,
    pub config: Config,
}

impl<Api> CoinbaseFetcher<Api> {
    fn sign_request(
        &self,
        mut headers: HeaderMap,
        method: &'static str,
        endpoint: &str,
        params: Option<&Query>,
        body: &str,
        base64_encode: bool,
    ) -> HeaderMap {
        headers.insert(
            "CB-ACCESS-KEY",
            HeaderValue::from_str(&self.credentials.key).unwrap(),
        );
        headers.append(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_static("reqwest/0.11"),
        );
        headers.insert("CB-VERSION", HeaderValue::from_static("2020-05-23"));
        let timestamp = Utc::now().timestamp();
        headers.insert(
            "CB-ACCESS-TIMESTAMP",
            HeaderValue::from_str(&timestamp.to_string()).unwrap(),
        );
        let uri_path = match params {
            Some(params) => format!("{}?{}", endpoint, params.materialize().full_query),
            None => endpoint.to_string(),
        };

        let secret_key = match base64_encode {
            true => base64::decode(self.credentials.secret.as_bytes()).unwrap(),
            false => self.credentials.secret.as_bytes().to_vec(),
        };

        if let Some(passphrase) = self.credentials.passphrase.as_ref() {
            headers.insert(
                "CB-ACCESS-PASSPHRASE",
                HeaderValue::from_str(passphrase).unwrap(),
            );
        }

        let mut signed_key = Hmac::<Sha256>::new_from_slice(&secret_key).unwrap();
        signed_key.update(format!("{}{}{}{}", timestamp, method, uri_path, body).as_ref());
        let signature: String = match base64_encode {
            true => base64::encode(signed_key.finalize().into_bytes()).to_string(),
            false => hex_encode(signed_key.finalize().into_bytes()),
        };
        headers.insert("CB-ACCESS-SIGN", HeaderValue::from_str(&signature).unwrap());

        headers
    }

    fn from_json<T: DeserializeOwned>(&self, resp_bytes: &Bytes) -> Result<T> {
        match serde_json::from_slice(resp_bytes) {
            Ok(val) => Ok(val),
            Err(err) => Err(Error::new(err).context(format!(
                "couldn't parse response from API: \n{:?}",
                resp_bytes
            ))),
        }
    }
}

impl CoinbaseFetcher<Std> {
    pub fn new(config: Config) -> Self {
        Self {
            api_client: ApiClient::new(),
            credentials: Credentials::<Std>::new(),
            api: PhantomData,
            config,
        }
    }

    async fn fetch_paginated_resource<T: Identifiable<String> + DeserializeOwned>(
        &self,
        endpoint: String,
    ) -> Result<Vec<T>> {
        let mut all_resources = Vec::new();
        let mut params = Query::new();
        params.cached_param("limit", 100);

        loop {
            // loop until all pages are consumed
            let headers =
                self.sign_request(HeaderMap::new(), "GET", &endpoint, Some(&params), "", false);
            let resp = self
                .api_client
                .make_request(
                    &format!("{}{}", ApiEndpoint::Std, endpoint),
                    Some(params),
                    Some(headers),
                    true,
                )
                .await?;
            let parsed_resp: Response<T> = self.from_json(&resp.bytes)?;
            all_resources.extend(parsed_resp.data);
            if parsed_resp.pagination.next_uri.is_some() {
                // order of params matters for signature, i.e. `limit` has to go first
                params = Query::new();
                params.cached_param("limit", 100);
                if let Some(last) = all_resources.last() {
                    params.cached_param("starting_after", last.id());
                }
            } else {
                break;
            }
        }
        Ok(all_resources)
    }

    async fn fetch_resource_for_accounts(
        &self,
        endpoint: StdEndpoints,
    ) -> Result<Vec<Transaction>> {
        // let mut handles = Vec::new();
        let mut all_trans = Vec::new();
        for acc in self
            .fetch_paginated_resource::<Account>(StdEndpoints::Accounts.to_string())
            .await?
        {
            // only fetch for accounts specified in the config file
            if !self.config.symbols.contains(&acc.currency.code) {
                continue;
            }

            match self
                .fetch_paginated_resource(endpoint.to_string().replace("{account_id}", &acc.id))
                .await
            {
                // could join_all(handles) instead of this but the API complains with
                // too many requests.
                Ok(x) => all_trans.extend(x),
                Err(err) => match err.downcast_ref::<ApiError>() {
                    // ignore resources not found
                    Some(ApiError::NotFound) => (),
                    // any other error just pass it through
                    Some(_) => return Err(err),
                    None => (),
                },
            }
        }
        Ok(all_trans)
    }

    pub async fn fetch_transactions(&self) -> Result<Vec<Transaction>> {
        self.fetch_resource_for_accounts(StdEndpoints::Transactions)
            .await
    }

    pub async fn fetch_buys(&self) -> Result<Vec<Transaction>> {
        self.fetch_resource_for_accounts(StdEndpoints::Buys).await
    }

    pub async fn fetch_sells(&self) -> Result<Vec<Transaction>> {
        self.fetch_resource_for_accounts(StdEndpoints::Sells).await
    }

    pub async fn fetch_fiat_deposits(&self) -> Result<Vec<Transaction>> {
        self.fetch_resource_for_accounts(StdEndpoints::Deposits)
            .await
    }

    pub async fn fetch_withdraws(&self) -> Result<Vec<Transaction>> {
        self.fetch_resource_for_accounts(StdEndpoints::Withdraws)
            .await
    }
}

impl<'a> CoinbaseFetcher<Pro> {
    pub fn new(config: Config) -> Self {
        Self {
            api_client: ApiClient::new(),
            credentials: Credentials::<Pro>::new(),
            api: PhantomData,
            config,
        }
    }

    pub async fn fetch_products(&self) -> Result<Vec<Product>> {
        let headers = self.sign_request(
            HeaderMap::new(),
            "GET",
            ProEndpoints::Products.into(),
            None,
            "",
            true,
        );
        let resp = self
            .api_client
            .make_request(
                &format!("{}{}", ApiEndpoint::Pro, ProEndpoints::Products),
                None,
                Some(headers),
                true,
            )
            .await?;
        let parsed_resp: Vec<Product> = self.from_json(&resp.bytes)?;
        Ok(parsed_resp)
    }

    pub async fn fetch_fills(&self, product_id: &str) -> Result<Vec<Fill>> {
        let mut start_date = self.config.start_date;
        let mut end_date = start_date + Duration::days(30);
        let mut all_fills = Vec::new();

        loop {
            let today = Utc::now().naive_utc().date();
            let mut last_id = None;
            loop {
                // loop until all paginated elements are consumed
                let mut params = Query::new();
                params
                    .cached_param("product_id", product_id.clone())
                    .cached_param("start_date", start_date)
                    .cached_param("end_date", end_date)
                    .cached_param("limit", 1000);
                if let Some(last_id) = last_id {
                    params.cached_param("after", last_id);
                }
                let headers = self.sign_request(
                    HeaderMap::new(),
                    "GET",
                    ProEndpoints::Fills.into(),
                    Some(&params),
                    "",
                    true,
                );
                let resp = self
                    .api_client
                    .make_request(
                        &format!("{}{}", ApiEndpoint::Pro, ProEndpoints::Fills),
                        Some(params),
                        Some(headers),
                        true,
                    )
                    .await?;
                let parsed_resp: Vec<Fill> = self.from_json(&resp.bytes)?;
                let resp_len = parsed_resp.len();
                if resp_len >= 1000 {
                    last_id = Some(parsed_resp.last().unwrap().trade_id);
                }
                all_fills.extend(parsed_resp);
                if resp_len < 1000 {
                    break;
                }
            }

            start_date = start_date + Duration::days(30);
            end_date = end_date + Duration::days(30);
            // loop until end_date is >= today
            end_date = std::cmp::min(today, end_date);
            if end_date == today {
                break;
            }
        }
        Ok(all_fills)
    }
}
