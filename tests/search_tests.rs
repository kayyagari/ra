use std::fs;
use std::fs::File;
use std::io::Read;
use rocket::http::Header;
use rocket::local::blocking::Client;
use serde_json::Value;
use ra_registry::utils::test_utils::*;

#[test]
fn test_query() {
    let tc = TestContainer::new();
    let r = tc.create_server_with_example_patient();
    let client = Client::tracked(r).expect("create a HTTP client");

    let resp = client.get("/metadata").dispatch();
    assert_eq!(200, resp.status().code);
    //let resp_val = resp.into_json::<Value>().unwrap();

    let resp = client.get("/Patient?name=Windsor").dispatch();
    assert_eq!(200, resp.status().code);
    let resp_val = resp.into_json::<Value>().unwrap();
    assert_eq!(1, resp_val.get("count").unwrap().as_i64().unwrap());
    assert_eq!("searchset", resp_val.get("type").unwrap().as_str().unwrap());
    assert_eq!("1974-12-25T14:35:45-05:00", resp_val.pointer("/entries/0/resource/_birthDate/extension/0/valueDateTime").unwrap().as_str().unwrap());

    let search_req = client.get("/Patient?unknown-search-param=1&name=Windsor").header(Header::new("Prefer", "handling=lenient"));
    let resp = search_req.dispatch();
    assert_eq!(200, resp.status().code);

    let search_req = client.get("/Patient?unknown-search-param=1&name=Windsor").header(Header::new("Prefer", "handling=strict"));
    let resp = search_req.dispatch();
    assert_eq!(400, resp.status().code);

    let bundle_file = fs::read("test_data/resources/bundle-example.json").unwrap();
    let resp = client.post("/").body(bundle_file.as_slice()).dispatch();
    assert_eq!(200, resp.status().code);
    let resp = client.get("/Patient?name=Windsor,Dusti191").dispatch();
    assert_eq!(200, resp.status().code);
    let resp_val = resp.into_json::<Value>().unwrap();
    assert_eq!(2, resp_val.get("count").unwrap().as_i64().unwrap());

    let resp = client.get("/Patient?name=Windsor,Dusti191&name=xyz").dispatch();
    assert_eq!(200, resp.status().code);
    let resp_val = resp.into_json::<Value>().unwrap();
    assert_eq!(0, resp_val.get("count").unwrap().as_i64().unwrap());
}