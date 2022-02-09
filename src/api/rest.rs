use std::convert::Infallible;
use std::io::{BufWriter, Cursor, Sink};
use log::debug;

use rocket::{Config, Data, get, post, Request, Response, State};
use rocket::data::{DataStream, FromData};
use rocket::form::{Form, FromForm};
use rocket::http::{ContentType, Header, Status};
use rocket::http::hyper::header::LAST_MODIFIED;
use rocket::request::{FromRequest, Outcome};
use rocket::response::content::Json;
use rocket::response::Responder;
use rocket::serde::Deserialize;
use serde_json::Value;

use crate::api::base::{ApiBase, ConditionalHeaders, RaResponse, ResponseHints, ReturnContent, SearchQuery};
use crate::bson_utils;
use crate::errors::RaError;

const FHIR_JSON: &'static str = "application/fhir+json";

#[rocket::async_trait]
impl<'r> FromRequest<'r> for SearchQuery<'r> {
    type Error = Infallible;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let mut params: Vec<(&'r str, &'r str)> = Vec::new();
        for item in request.query_fields() {
            match item.name.as_name().as_str() {
                "return" | "_pretty" | "_summary" | "_elements" | "_format" => {
                    continue;
                },
                name => {
                    params.push((name, item.value));
                }
            }
        }

        Outcome::Success(SearchQuery {params})
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for &'r ResponseHints {
    type Error = RaError;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let closure = || {
            let mut rturn = ReturnContent::Minimal;
            let mut pretty = false;
            let mut elements = false;
            let mut summary = false;
            for item in request.query_fields() {
                match item.name.as_name().as_str() {
                    "return" => {
                        rturn = ReturnContent::from(item.value);
                    },
                    "_pretty" => {
                        let tmp = item.value.parse::<bool>();
                        if let Ok(b) = tmp {
                            pretty = b;
                        }
                    },
                    "_summary" => {
                        let tmp = item.value.parse::<bool>();
                        if let Ok(b) = tmp {
                            summary = b;
                        }
                    },
                    "_elements" => {
                        let tmp = item.value.parse::<bool>();
                        if let Ok(b) = tmp {
                            elements = b;
                        }
                    },
                    _ => {
                        continue;
                    }
                }
            }
            ResponseHints{rturn, pretty, elements, summary}
        };

        Outcome::Success(request.local_cache(closure))
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ConditionalHeaders<'r> {
    type Error = RaError;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let mut h = request.headers().get("If-None-Exist");
        let mut ch = ConditionalHeaders{if_none_exist: h.next()};
        Outcome::Success(ch)
    }
}

#[rocket::async_trait]
impl<'r, 'o: 'r> Responder<'r, 'o> for RaError {
    fn respond_to(self, request: &'r Request<'_>) -> rocket::response::Result<'o> {
        match self {
            RaError::DbError(s) | RaError::SchemaParsingError(s) => {
                Response::build()
                    .sized_body(s.len(), Cursor::new(s))
                    .status(Status::InternalServerError)
                    .ok()
            },
            RaError::SchemaValidationError => {
                let msg = "schema validation failed";
                Response::build()
                    .sized_body(msg.len(), Cursor::new(msg))
                    .status(Status::InternalServerError)
                    .ok()
            },
            RaError::BadRequest(s) => {
                Response::build()
                    .sized_body(s.len(), Cursor::new(s))
                    .status(Status::BadRequest)
                    .ok()
            },
            RaError::NotFound(s) => {
                Response::build()
                    .sized_body(s.len(), Cursor::new(s))
                    .status(Status::NotFound)
                    .ok()
            }
        }
    }
}

impl<'r, 'o: 'r> Responder<'r, 'o> for RaResponse {
    fn respond_to(self, request: &'r Request<'_>) -> rocket::response::Result<'o> {
        let hints: &ResponseHints = request.local_cache(|| {ResponseHints::default()});
        debug!("{:?}", hints);

        match self {
            RaResponse::Created(doc) => {
                let id = doc.get_str("id").unwrap();
                let res_type = doc.get_str("resourceType").unwrap();
                let vid = bson_utils::get_int(&doc, "meta.versionId");

                // let cfg = request.rocket().config();
                let loc = format!("{}/{}/_history/{}", res_type, id, vid);

                let last_modified = bson_utils::get_time(&doc, "meta.lastUpdated").unwrap();
                //Last-Modified: <day-name>, <day> <month> <year> <hour>:<minute>:<second> GMT
                let last_modified = last_modified.format("%a, %d %m %Y %H:%M:%S GMT").to_string();

                Response::build()
                    .status(Status::Created)
                    .raw_header("Location", loc)
                    .raw_header("Etag", vid.to_string())
                    .raw_header("Last-Modified", last_modified)
                    .ok()
            },
            RaResponse::Success => {
                Response::build()
                    .status(Status::Ok)
                    .ok()
            },
            RaResponse::SearchResult(ss) => {
                let buf = serde_json::to_vec(&ss).unwrap();
                Response::build()
                    .status(Status::Ok)
                    .raw_header("Content-Type", FHIR_JSON)
                    .sized_body(buf.len(), Cursor::new(buf))
                    .ok()
            }
        }
    }
}

fn parse_input(d: &[u8]) -> Result<Value, RaError> {
    let val: serde_json::Result<Value> = serde_json::from_reader(d);
    if let Err(e) = val {
        return Err(RaError::bad_req(e.to_string()));
    }
    Ok(val.unwrap())
}

#[post("/<res_name>", data = "<data>")]
pub fn create(res_name: &str, data: &[u8], hints: &ResponseHints, ch: ConditionalHeaders<'_>, base: &State<ApiBase>) -> Result<RaResponse, RaError> {
    let val = parse_input(data)?;
    base.create(res_name, &val)
}

#[post("/", data = "<data>")]
pub fn bundle(data: &[u8], hints: &ResponseHints, base: &State<ApiBase>) -> Result<RaResponse, RaError> {
    let val = parse_input(data)?;
    base.bundle(val)
}

#[get("/<res_name>")]
pub fn search(res_name: &str, query: SearchQuery, hints: &ResponseHints, base: &State<ApiBase>) -> Result<RaResponse, RaError> {
    debug!("{:?}", query);
    base.search_query(res_name, &query, hints)
}
