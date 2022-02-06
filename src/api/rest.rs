use std::io::Cursor;

use rocket::{Config, Data, FromForm, post, Request, Response, State};
use rocket::data::FromData;
use rocket::http::{ContentType, Header, Status};
use rocket::http::hyper::header::LAST_MODIFIED;
use rocket::request::{FromRequest, Outcome};
use rocket::response::content::Json;
use rocket::response::Responder;
use rocket::serde::Deserialize;
use serde_json::Value;

use crate::api::base::{ApiBase, RaResponse};
use crate::bson_utils;
use crate::errors::RaError;

#[derive(FromForm)]
pub struct ResponseHints<'r> {
    #[field(name = "return")]
    rturn: &'r str,
    #[field(name = "_pretty")]
    pretty: bool,
    #[field(name = "_summary")]
    summary: bool,
    #[field(name = "_elements")]
    elements: bool,
}

pub enum ReturnContent {
    Minimal,
    Representation,
    OperationOutcome
}

impl ReturnContent {
    pub fn from<S: AsRef<str>>(s: S) -> Self {
        match s.as_ref() {
            "minimal" => ReturnContent::Minimal,
            "representation" => ReturnContent::Representation,
            "OperationOutcome" => ReturnContent::OperationOutcome,
            _ => ReturnContent::Minimal
        }
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
            }
        }
    }
}

impl<'r, 'o: 'r> Responder<'r, 'o> for RaResponse {
    fn respond_to(self, request: &'r Request<'_>) -> rocket::response::Result<'o> {
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
            }
        }
    }
}

fn parse_input(d: &str) -> Result<Value, RaError> {
    let val: serde_json::Result<Value> = serde_json::from_str(d);
    if let Err(e) = val {
        return Err(RaError::bad_req(e.to_string()));
    }
    Ok(val.unwrap())
}

#[post("/<res_name>?<hints..>", data = "<data>")]
pub fn create(res_name: &str, data: &str, hints: Option<ResponseHints<'_>>, base: &State<ApiBase>) -> Result<RaResponse, RaError> {
    let val = parse_input(data)?;
    base.create(res_name, &val)
}