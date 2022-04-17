use rocket::http::Header;
use rocket::local::blocking::Client;
use serde_json::Value;
use ra_registry::utils::test_utils::*;

#[test]
fn test_query() {
    let tc = TestContainer::new();
    let r = tc.create_server_with_example_patient();
    let client = Client::tracked(r).expect("create a HTTP client");
    let resp = client.get("/Patient?name=Windsor").dispatch();
    assert_eq!(200, resp.status().code);
    let bundle = resp.into_json::<Value>().unwrap();
    assert_eq!(1, bundle.get("count").unwrap().as_i64().unwrap());
    assert_eq!("searchset", bundle.get("type").unwrap().as_str().unwrap());
    assert_eq!("1974-12-25T14:35:45-05:00", bundle.pointer("/entries/0/resource/_birthDate/extension/0/valueDateTime").unwrap().as_str().unwrap());

    let mut search_req = client.get("/Patient?unknown-search-param=1&name=Windsor").header(Header::new("Prefer", "handling=lenient"));
    let resp = search_req.dispatch();
    assert_eq!(200, resp.status().code);

    let mut search_req = client.get("/Patient?unknown-search-param=1&name=Windsor").header(Header::new("Prefer", "handling=strict"));
    let resp = search_req.dispatch();
    assert_eq!(400, resp.status().code);
}