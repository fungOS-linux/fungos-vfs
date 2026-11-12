//! An S3 bucket as a [`Replica`], signed the way R2 and AWS both expect.
//!
//! SigV4 is the part that has to be exactly right and is therefore the part that
//! is tested against published vectors rather than against a live bucket: a
//! signature that is *nearly* right fails with a 403 whose body explains nothing.
//! [`Credentials::sign`] is pure — it takes the request parts and the timestamp
//! and returns the headers — so the test vectors can pin it without a network.
//!
//! Two things this deliberately does not do:
//!
//! - it does not invent a key layout to save a round trip; a bucket holds
//!   `<first two hex>/<hex>`, the same shape as the local store and a disk replica,
//!   so anything that can read one mirror can read the others by hand;
//! - it does not hide the credential source. `R2_ACCESS_KEY_ID`,
//!   `R2_SECRET_ACCESS_KEY` and `R2_ENDPOINT` come from the environment, and a
//!   missing one is a configuration error rather than a request that fails later.

use std::time::Duration;

use fungos_vfs::{ContentId, FetchedRange, Replica, Stored, VfsError};
use ring::{
    digest,
    hmac::{self, Key},
};

pub const ACCESS_KEY_ENV: &str = "R2_ACCESS_KEY_ID";
pub const SECRET_KEY_ENV: &str = "R2_SECRET_ACCESS_KEY";
pub const ENDPOINT_ENV: &str = "R2_ENDPOINT";
pub const REGION_ENV: &str = "R2_REGION";

/// The credential pair and region a request is signed with.
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

/// The parts of a request that the signature covers.
pub struct CanonicalRequest<'a> {
    pub method: &'a str,
    /// The path, before encoding.
    pub path: &'a str,
    /// Query parameters, in any order; they are sorted and encoded here.
    pub query: Vec<(&'a str, &'a str)>,
    /// Headers the signature covers. `host` must be among them.
    pub headers: Vec<(&'a str, String)>,
    /// The hex SHA-256 of the body, or the hash of nothing for a bodyless request.
    pub payload_hash: &'a str,
}

/// What a signature produced, including the intermediate strings so a failing
/// request can be explained instead of guessed at.
#[derive(Debug)]
pub struct Signed {
    pub authorization: String,
    pub signed_headers: String,
    pub canonical_request: String,
    pub string_to_sign: String,
}

/// The SHA-256 of an empty body: what a GET or a HEAD signs.
pub fn empty_payload_hash() -> String {
    hex(digest::digest(&digest::SHA256, b"").as_ref())
}

pub fn payload_hash(bytes: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, bytes).as_ref())
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Percent-encode everything but the unreserved set, which is what SigV4 means by
/// "URI-encoded". Keys here are hex, so this is a correctness detail rather than a
/// common path — and it is still done, because a key that needs encoding is
/// exactly the one that would otherwise sign differently from how it is sent.
fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char);
            }
            b'/' => out.push('/'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

fn hmac_hex(key: &[u8], data: &[u8]) -> String {
    let key = Key::new(hmac::HMAC_SHA256, key);
    hex(hmac::sign(&key, data).as_ref())
}

fn hmac_bytes(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, data).as_ref().to_vec()
}

impl Credentials {
    /// Sign a request for `amz_date` — `20130524T000000Z`, passed in rather than
    /// read from the clock so the test vectors can pin it.
    pub fn sign(&self, request: &CanonicalRequest<'_>, amz_date: &str) -> Result<Signed, VfsError> {
        let date = amz_date
            .get(..8)
            .ok_or_else(|| VfsError::Config(format!("timestamp {amz_date:?} is not IS08601")))?;

        let mut headers = request.headers.clone();
        if !headers.iter().any(|(name, _)| *name == "host") {
            return Err(VfsError::Config(
                "a signed request must include the host header".to_owned(),
            ));
        }
        headers.sort_by(|left, right| left.0.cmp(right.0));
        let canonical_headers: String = headers
            .iter()
            .map(|(name, value)| format!("{}:{}\n", name.to_ascii_lowercase(), value.trim()))
            .collect();
        let signed_headers = headers
            .iter()
            .map(|(name, _)| name.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(";");

        let mut query = request
            .query
            .iter()
            .map(|(name, value)| (encode(name), encode(value)))
            .collect::<Vec<_>>();
        query.sort();
        let canonical_query = query
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("&");

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            request.method,
            encode(request.path),
            canonical_query,
            canonical_headers,
            signed_headers,
            request.payload_hash
        );

        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex(digest::digest(&digest::SHA256, canonical_request.as_bytes()).as_ref())
        );

        let mut key = hmac_bytes(
            format!("AWS4{}", self.secret_key).as_bytes(),
            date.as_bytes(),
        );
        key = hmac_bytes(&key, self.region.as_bytes());
        key = hmac_bytes(&key, b"s3");
        key = hmac_bytes(&key, b"aws4_request");
        let signature = hmac_hex(&key, string_to_sign.as_bytes());

        Ok(Signed {
            authorization: format!(
                "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, \
                 Signature={signature}",
                self.access_key
            ),
            signed_headers,
            canonical_request,
            string_to_sign,
        })
    }
}

/// A bucket as content storage.
pub struct R2Replica {
    name: String,
    endpoint: String,
    bucket: String,
    credentials: Credentials,
    client: reqwest::blocking::Client,
    writable: bool,
}

/// A timestamp in the shape SigV4 wants, from the system clock.
fn now_stamp() -> Result<(String, String), VfsError> {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| VfsError::Config(format!("clock before the epoch: {error}")))?
        .as_secs();
    // Days since the epoch, then a civil date: no dependency for two conversions.
    let days = i64::try_from(seconds / 86_400).unwrap_or(0);
    let (year, month, day) = civil_from_days(days);
    let second_of_day = seconds % 86_400;
    let stamp = format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        second_of_day / 3600,
        (second_of_day % 3600) / 60,
        second_of_day % 60
    );
    Ok((stamp.clone(), stamp))
}

/// Howard Hinnant's days-to-civil-date, the standard loop-free conversion.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = u32::try_from(day_of_year - (153 * month_position + 2) / 5 + 1).unwrap_or(1);
    let month = u32::try_from(if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    })
    .unwrap_or(1);
    (year + i64::from(month <= 2), month, day)
}

impl R2Replica {
    pub fn new(
        name: impl Into<String>,
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        credentials: Credentials,
    ) -> Self {
        Self {
            name: name.into(),
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            bucket: bucket.into(),
            credentials,
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default(),
            writable: true,
        }
    }

    /// Credentials from the environment, refusing to invent any of the three
    /// facts a request needs.
    pub fn from_env(name: impl Into<String>, bucket: impl Into<String>) -> Result<Self, VfsError> {
        let read = |variable: &str| {
            std::env::var(variable).map_err(|_| {
                VfsError::Config(format!(
                    "{variable} is not set: the bucket has no credentials"
                ))
            })
        };
        let credentials = Credentials {
            access_key: read(ACCESS_KEY_ENV)?,
            secret_key: read(SECRET_KEY_ENV)?,
            region: std::env::var(REGION_ENV).unwrap_or_else(|_| "auto".to_owned()),
        };
        Ok(Self::new(name, read(ENDPOINT_ENV)?, bucket, credentials))
    }

    #[must_use]
    pub fn read_only(mut self) -> Self {
        self.writable = false;
        self
    }

    /// Where `id` lives in the bucket: the same sharded layout as the local store
    /// and a disk replica.
    #[must_use]
    pub fn key_for(id: &ContentId) -> String {
        let hex = id.hex();
        format!("{}/{hex}", &hex[..2])
    }

    fn url(&self, key: &str) -> String {
        format!("{}/{}/{}", self.endpoint, self.bucket, key)
    }

    fn host(&self) -> Result<String, VfsError> {
        let url = reqwest::Url::parse(&self.endpoint)
            .map_err(|error| VfsError::Config(format!("{}: {error}", self.endpoint)))?;
        url.host_str()
            .map(|host| match url.port() {
                Some(port) => format!("{host}:{port}"),
                None => host.to_owned(),
            })
            .ok_or_else(|| VfsError::Config(format!("{} has no host", self.endpoint)))
    }

    /// One signed request, carrying the body for a write.
    fn send(
        &self,
        method: &str,
        key: &str,
        extra: Vec<(&str, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<reqwest::blocking::Response, VfsError> {
        let (stamp, _) = now_stamp()?;
        let payload = body.clone().unwrap_or_default();
        let hash = payload_hash(&payload);
        let mut headers = vec![
            ("host", self.host()?),
            ("x-amz-content-sha256", hash.clone()),
            ("x-amz-date", stamp.clone()),
        ];
        headers.extend(extra.iter().map(|(name, value)| (*name, value.clone())));

        let signed = self.credentials.sign(
            &CanonicalRequest {
                method,
                path: &format!("/{}/{}", self.bucket, key),
                query: Vec::new(),
                headers: headers.clone(),
                payload_hash: &hash,
            },
            &stamp,
        )?;

        let mut request = self.client.request(
            reqwest::Method::from_bytes(method.as_bytes())
                .map_err(|error| VfsError::Config(format!("method {method}: {error}")))?,
            self.url(key),
        );
        for (name, value) in &headers {
            if *name == "host" {
                continue;
            }
            request = request.header(*name, value.as_str());
        }
        request = request
            .header("x-amz-date", stamp.as_str())
            .header("authorization", signed.authorization.as_str());
        if let Some(body) = body {
            request = request.body(body);
        }
        request
            .send()
            .map_err(|error| VfsError::Transport(format!("{} {method} {key}: {error}", self.name)))
    }

    fn missing(&self, id: &ContentId) -> VfsError {
        VfsError::NotHydrated(format!("{} does not hold {id}", self.name))
    }
}

impl Replica for R2Replica {
    fn name(&self) -> &str {
        &self.name
    }

    fn fetch(&self, id: &ContentId, _size: u64) -> Result<Vec<u8>, VfsError> {
        let key = Self::key_for(id);
        let response = self.send("GET", &key, Vec::new(), None)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(self.missing(id));
        }
        let response = response
            .error_for_status()
            .map_err(|error| VfsError::Transport(format!("{} GET {key}: {error}", self.name)))?;
        let bytes = response
            .bytes()
            .map_err(|error| VfsError::Transport(format!("{} GET {key}: {error}", self.name)))?
            .to_vec();
        if ContentId::from_bytes(&bytes) != *id {
            return Err(VfsError::InvalidContentId(format!(
                "{} holds bytes under {key} that hash to something else",
                self.name
            )));
        }
        Ok(bytes)
    }

    /// A ranged GET. A bucket answers ranges natively, which is the difference
    /// between one 64 KiB request and one whole-object request per range.
    fn fetch_range(
        &self,
        id: &ContentId,
        offset: u64,
        length: u32,
    ) -> Result<FetchedRange, VfsError> {
        let key = Self::key_for(id);
        let end = offset.saturating_add(u64::from(length)).saturating_sub(1);
        let response = self.send(
            "GET",
            &key,
            vec![("range", format!("bytes={offset}-{end}"))],
            None,
        )?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(self.missing(id));
        }
        let status = response.status();
        let response = response
            .error_for_status()
            .map_err(|error| VfsError::Transport(format!("{} GET {key}: {error}", self.name)))?;
        let bytes = response
            .bytes()
            .map_err(|error| VfsError::Transport(format!("{} GET {key}: {error}", self.name)))?
            .to_vec();
        // 200 means the bucket ignored the range and sent everything, which is a
        // correct answer to a question nobody asked: slice it here.
        Ok(if status == reqwest::StatusCode::PARTIAL_CONTENT {
            FetchedRange {
                eof: bytes.len() < length as usize,
                bytes,
            }
        } else {
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            let stop = start.saturating_add(length as usize).min(bytes.len());
            FetchedRange {
                eof: stop >= bytes.len(),
                bytes: bytes[start..stop].to_vec(),
            }
        })
    }

    fn put(&self, id: &ContentId, bytes: &[u8]) -> Result<Stored, VfsError> {
        if !self.writable {
            return Err(VfsError::ReadOnly);
        }
        if ContentId::from_bytes(bytes) != *id {
            return Err(VfsError::InvalidContentId(format!(
                "{id} does not match the bytes sent"
            )));
        }
        let key = Self::key_for(id);
        // A HEAD first, so a repeat backup costs one request instead of a full
        // upload and reports honestly.
        if self
            .send("HEAD", &key, Vec::new(), None)
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(Stored::AlreadyPresent);
        }
        self.send(
            "PUT",
            &key,
            vec![("content-type", "application/octet-stream".to_owned())],
            Some(bytes.to_vec()),
        )?
        .error_for_status()
        .map_err(|error| VfsError::Transport(format!("{} PUT {key}: {error}", self.name)))?;
        Ok(Stored::Added)
    }

    fn writable(&self) -> bool {
        self.writable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The credentials and timestamp from the AWS SigV4 examples, so the vectors
    /// below mean something.
    fn example() -> Credentials {
        Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_owned(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned(),
            region: "us-east-1".to_owned(),
        }
    }

    /// AWS's published `GET Object` example, which botocore 1.34.69 also reproduces
    /// exactly — two independent sources agreeing on one vector is what makes it
    /// worth pinning.
    #[test]
    fn the_published_get_object_vector_reproduces() {
        let credentials = example();
        let payload = empty_payload_hash();
        let signed = credentials
            .sign(
                &CanonicalRequest {
                    method: "GET",
                    path: "/test.txt",
                    query: Vec::new(),
                    headers: vec![
                        ("host", "examplebucket.s3.amazonaws.com".to_owned()),
                        ("range", "bytes=0-9".to_owned()),
                        ("x-amz-content-sha256", payload.clone()),
                        ("x-amz-date", "20130524T000000Z".to_owned()),
                    ],
                    payload_hash: &payload,
                },
                "20130524T000000Z",
            )
            .unwrap();
        assert!(
            signed.authorization.ends_with(
                "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
            ),
            "{}",
            signed.authorization
        );
        assert_eq!(
            signed.signed_headers,
            "host;range;x-amz-content-sha256;x-amz-date"
        );
    }

    /// A `PUT` with a body and a signed non-`x-amz` header.
    ///
    /// The expected signature is **not** from memory: it is what botocore 1.34.69
    /// produces for these exact request parts, computed with a frozen clock
    /// (`20130524T000000Z`) and
    /// `S3SigV4Auth(creds, "s3", "us-east-1").signature(...)`. A remembered
    /// constant is how a signing test ends up asserting a bug.
    #[test]
    fn a_put_with_a_body_signs_the_way_botocore_does() {
        let credentials = example();
        let hash = payload_hash(b"Welcome to Amazon S3.");
        let signed = credentials
            .sign(
                &CanonicalRequest {
                    method: "PUT",
                    path: "/test$file.text",
                    query: Vec::new(),
                    headers: vec![
                        ("host", "examplebucket.s3.amazonaws.com".to_owned()),
                        ("x-amz-content-sha256", hash.clone()),
                        ("x-amz-date", "20130524T000000Z".to_owned()),
                        ("x-amz-storage-class", "REDUCED_REDUNDANCY".to_owned()),
                    ],
                    payload_hash: &hash,
                },
                "20130524T000000Z",
            )
            .unwrap();
        assert_eq!(
            hash,
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
        );
        assert!(
            signed.authorization.ends_with(
                "Signature=1ee3a9a719bf9cd67d34043a52b3d1f8b674e378dc99c0748019b43f49b5b9bb"
            ),
            "{}",
            signed.authorization
        );
        assert_eq!(
            signed.signed_headers,
            "host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"
        );
    }

    #[test]
    fn the_key_layout_matches_a_disk_replica() {
        let id = ContentId::from_bytes(b"same shape everywhere");
        let key = R2Replica::key_for(&id);
        assert_eq!(key, format!("{}/{}", &id.hex()[..2], id.hex()));
    }

    #[test]
    fn a_signature_without_a_host_is_refused() {
        let error = example()
            .sign(
                &CanonicalRequest {
                    method: "GET",
                    path: "/x",
                    query: Vec::new(),
                    headers: Vec::new(),
                    payload_hash: &empty_payload_hash(),
                },
                "20130524T000000Z",
            )
            .unwrap_err();
        assert!(format!("{error}").contains("host"), "{error}");
    }

    #[test]
    fn the_clock_converts_to_a_stamp_in_the_sigv4_shape() {
        let (stamp, _) = now_stamp().unwrap();
        assert_eq!(stamp.len(), 16, "{stamp}");
        assert!(stamp.ends_with('Z'), "{stamp}");
        assert_eq!(&stamp[8..9], "T", "{stamp}");
        // 2024-02-29 and a leap year boundary, because date maths is where this
        // kind of code goes wrong.
        assert_eq!(civil_from_days(19_782), (2024, 2, 29));
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }
}
