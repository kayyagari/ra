use std::io::Cursor;

use rocket::{Data, FromForm, post, Request, Response, State};
use rocket::data::FromData;
use rocket::http::{ContentType, Header, Status};
use rocket::request::{FromRequest, Outcome};
use rocket::response::content::Json;
use rocket::response::Responder;
use rocket::serde::Deserialize;
use serde_json::Value;

use crate::api::base::{ApiBase, RaResponse};
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
impl<'r> Responder<'r, 'static> for RaError {
    fn respond_to(self, request: &'r Request<'_>) -> rocket::response::Result<'static> {
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
                    .status(Status::BadRequest)
                    .ok()
            },
            RaError::InvalidValueError(s) => {
                Response::build()
                    .sized_body(s.len(), Cursor::new(s))
                    .status(Status::BadRequest)
                    .ok()
            }
        }
    }
}

impl<'r> Responder<'r, 'static> for RaResponse {
    fn respond_to(self, request: &'r Request<'_>) -> rocket::response::Result<'static> {
        match self {
            RaResponse::Created(doc) => {
                let id = doc.get("id").unwrap().as_str().unwrap();
                Response::build()
                    .status(Status::Created)
                    // .header(Header::new("Location", id))
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
        return Err(RaError::invalid_err(e.to_string()));
    }
    Ok(val.unwrap())
}

#[post("/<res_name>?<hints..>", data = "<data>")]
pub fn create(res_name: &str, data: &str, hints: ResponseHints<'_>, base: &State<ApiBase>) -> Result<RaResponse, RaError> {
    let val = parse_input(data)?;
    base.create(res_name, &val)
}