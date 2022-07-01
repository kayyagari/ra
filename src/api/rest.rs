use std::convert::Infallible;
use std::io::{BufWriter, Cursor, Sink};
use std::process::exit;
use chrono::Utc;
use log::debug;

use rocket::{Build, Config, Data, get, post, Request, Response, Rocket, routes, State, warn};
use rocket::data::{DataStream, FromData};
use rocket::fairing::AdHoc;
use rocket::form::{Form, FromForm};
use rocket::http::{ContentType, Header, Status};
use rocket::http::hyper::header::LAST_MODIFIED;
use rocket::request::{FromRequest, Outcome};
use rocket::response::content::Json;
use rocket::response::Responder;
use rocket::serde::Deserialize;
use serde_json::Value;

use crate::api::base::{ApiBase, ConditionalHeaders, Contained, ContainedType, OperationOutcome, RaResponse, ResponseHints, ReturnContent, SearchQuery, Total};
use crate::utils::bson_utils;
use crate::errors::{IssueType, RaError};

const FHIR_JSON: &'static str = "application/fhir+json";
const DATE_HEADER_FORMAT: &'static str = "%a, %d %m %Y %H:%M:%S GMT";

#[rocket::async_trait]
impl<'r> FromRequest<'r> for SearchQuery<'r> {
    type Error = Infallible;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let mut params: Vec<(&'r str, &'r str)> = Vec::new();
        let mut sort: Option<&'r str> = None;
        let mut count: u32 = 20;
        let mut include: Option<&'r str> = None;
        let mut revinclude: Option<&'r str> = None;
        let mut total = Total::None;
        let mut contained = Contained::DoNotReturn;
        let mut contained_type = ContainedType::Container;
        let mut elements = false;
        let mut summary = false;
        let mut ignore_unknown_params = false;

        for item in request.query_fields() {
            match item.name.as_name().as_str() {
                "return" | "_pretty" | "_format" => {
                    continue;
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
                "_sort" => {
                    sort = Some(item.value);
                },
                "_count" => {
                    let tmp = item.value.parse::<u32>();
                    if let Err(e) = tmp {
                        // TODO what is the best way to handle errors here? just throw 400?
                        debug!("invalid value {} given for _count parameter ({})", item.value, e.to_string());
                    }
                    else {
                        count = tmp.unwrap();
                    }
                },
                "_include" => {
                    include = Some(item.value);
                },
                "_revinclude" => {
                    revinclude = Some(item.value);
                },
                "_total" => {
                    total = Total::from(item.value);
                },
                "_contained" => {
                    contained = Contained::from(item.value);
                },
                "_containedType" => {
                    contained_type = ContainedType::from(item.value);
                },
                name => {
                    params.push((name, item.value));
                }
            }
        }

        for item in request.headers().get("Prefer") {
            let mut parts = item.splitn(2, "=");
            if let Some(h) = parts.next() {
                if h == "handling" {
                    if let Some(v) = parts.next() {
                        if v == "strict" {
                            ignore_unknown_params = false;
                        }
                        else if v == "lenient" {
                            ignore_unknown_params = true;
                        }
                    }
                    break;
                }
            }
        }

        let sq = SearchQuery {params, sort, count, include, revinclude, summary, total, elements, contained, contained_type, ignore_unknown_params};
        Outcome::Success(sq)
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

            for item in request.headers().get("Prefer") {
                if item.starts_with("return=") {
                    rturn = ReturnContent::from(item);
                    break;
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
        let now = Utc::now().format(DATE_HEADER_FORMAT).to_string();
        let status: Status;
        let oo: OperationOutcome;

        match self {
            RaError::DbError(s) | RaError::SchemaParsingError(s) => {
                oo = OperationOutcome::new_error(IssueType::Exception, s);
                status = Status::InternalServerError;
            },
            RaError::SchemaValidationError => {
                oo = OperationOutcome::new_error(IssueType::Processing, "schema validation failed");
                status = Status::InternalServerError;
            },
            RaError::SearchParamParsingError(s) => {
                oo = OperationOutcome::new_error(IssueType::Processing, s);
                status = Status::InternalServerError;
            },
            RaError::BadRequest(s) => {
                oo = OperationOutcome::new_error(IssueType::Processing, s);
                status = Status::BadRequest;
            },
            RaError::NotFound(s) => {
                oo = OperationOutcome::new_error(IssueType::Not_found, s);
                status = Status::NotFound;
            },
            RaError::Custom {code, outcome} => {
                oo = outcome;
                if let Some(code) = Status::from_code(code) {
                    status = code;
                }
                else {
                    // fallback to 500 if the code is unknown
                    status = Status::InternalServerError;
                }
            }
        }

        let mut resp = Response::build_from(Response::new());
        resp.raw_header("Date", now);
        let s = oo.serialize();
        resp.sized_body(s.len(), Cursor::new(s))
            .status(status)
            .ok()
    }
}

impl<'r, 'o: 'r> Responder<'r, 'o> for RaResponse {
    fn respond_to(self, request: &'r Request<'_>) -> rocket::response::Result<'o> {
        let hints: &ResponseHints = request.local_cache(|| {ResponseHints::default()});
        debug!("{:?}", hints);

        let now = Utc::now().format(DATE_HEADER_FORMAT).to_string();
        let mut resp = Response::build_from(Response::new());
        resp.raw_header("Date", now);

        match self {
            RaResponse::Created(doc) => {
                let id = doc.get_str("id").unwrap();
                let res_type = doc.get_str("resourceType").unwrap();
                let vid = bson_utils::get_int(&doc, "meta.versionId");

                // let cfg = request.rocket().config();
                let loc = format!("{}/{}/_history/{}", res_type, id, vid);

                let last_modified = bson_utils::get_time(&doc, "meta.lastUpdated").unwrap();
                //Last-Modified: <day-name>, <day> <month> <year> <hour>:<minute>:<second> GMT
                let last_modified = last_modified.format(DATE_HEADER_FORMAT).to_string();

                resp.status(Status::Created)
                    .raw_header("Location", loc)
                    .raw_header("Etag", vid.to_string())
                    .raw_header("Last-Modified", last_modified);

                if hints.rturn == ReturnContent::Representation {
                    let buf;
                    if hints.pretty {
                        buf = serde_json::to_vec_pretty(&doc).unwrap();
                    }
                    else {
                        buf = serde_json::to_vec(&doc).unwrap();
                    }
                    resp.sized_body(buf.len(), Cursor::new(buf));
                }

                resp.ok()
            },
            RaResponse::Success => {
                resp.status(Status::Ok)
                    .ok()
            },
            RaResponse::SearchResult(ss) => {
                let buf = serde_json::to_vec(&ss).unwrap();
                resp.status(Status::Ok)
                    .raw_header("Content-Type", FHIR_JSON)
                    .sized_body(buf.len(), Cursor::new(buf))
                    .ok()
            }
        }
    }
}

pub fn mount(api_base: ApiBase, config: Config) -> Result<Rocket<Build>, anyhow::Error> {
    let base_url = url::Url::parse(&api_base.base_url);
    if let Err(e) = base_url {
        let msg = format!("invalid base URL {}", &api_base.base_url);
        warn!("{}", &msg);
        return Err(anyhow::Error::msg(msg));
    }
    let base = base_url.unwrap().path().to_string();
    let mut server = rocket::build().manage(api_base).configure(config);
    server = server.attach(AdHoc::on_request("Create trace ID", |req, _| Box::pin(async move {
        log_mdc::insert("request_id", uuid::Uuid::new_v4().to_string());
    }
    )));
    Ok(server.mount(base, routes![create, bundle, search]))
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
