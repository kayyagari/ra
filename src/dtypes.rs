use lazy_static::lazy_static;
use std::collections::HashMap;

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum DataType {
    // primitive types
    BASE64BINARY,
    BOOLEAN,
    CANONICAL,
    CODE,
    DATE,
    DATETIME,
    DECIMAL,
    ID,
    INSTANT,
    INTEGER,
    MARKDOWN,
    NUMBER,
    OID,
    POSITIVEINT,
    STRING,
    TIME,
    UNSIGNEDINT,
    URI,
    URL,
    UUID,

    // complex types
    ADDRESS,
    AGE,
    ANNOTATION,
    ATTACHMENT,
    CODEABLECONCEPT,
    CODING,
    CONTACTPOINT,
    COUNT,
    DISTANCE,
    DURATION,
    HUMANNAME,
    IDENTIFIER,
    MONEY,
    PERIOD,
    QUANTITY,
    RANGE,
    RATIO,
    REFERENCE,
    SAMPLEDDATA,
    SIGNATURE,
    TIMING,

    // MetaDataTypes
    CONTACTDETAIL,
    CONTRIBUTOR,
    DATAREQUIREMENT,
    EXPRESSION,
    PARAMETERDEFINITION,
    RELATEDARTIFACT,
    TRIGGERDEFINITION,
    USAGECONTEXT,

    // Special types
    DOSAGE,
    META,

    // other types
    EXTENSION,

    // Element is also treated as the datatype for all complex attributes
    ELEMENT
}

impl DataType {
    pub fn is_primitive(&self) -> bool {
        match self {
            DataType::BASE64BINARY | DataType::BOOLEAN | DataType::CANONICAL | DataType::CODE |
            DataType::DATE | DataType::DATETIME | DataType::DECIMAL | DataType::ID |
            DataType::INSTANT | DataType::INTEGER | DataType::MARKDOWN | DataType::OID |
            DataType::POSITIVEINT | DataType::STRING | DataType::TIME | DataType::UNSIGNEDINT |
            DataType::URI | DataType::URL | DataType::UUID | DataType::NUMBER => {
                true
            },
            _ => {
                false
            }
        }
    }

    pub fn from_str(name: &str) -> Self {
        let t = dtypes.get(name);
        if t.is_some() {
            return *t.unwrap();
        }

        return DataType::ELEMENT;
    }
}

lazy_static! {
  static ref dtypes: HashMap<&'static str, DataType> = {
        let mut types = HashMap::new();
        types.insert("base64Binary", DataType::BASE64BINARY);
        types.insert("boolean", DataType::BOOLEAN);
        types.insert("canonical", DataType::CANONICAL);
        types.insert("code", DataType::CODE);
        types.insert("date", DataType::DATE);
        types.insert("dateTime", DataType::DATETIME);
        types.insert("decimal", DataType::DECIMAL);
        types.insert("id", DataType::ID);
        types.insert("instant", DataType::INSTANT);
        types.insert("integer", DataType::INTEGER);
        types.insert("markdown", DataType::MARKDOWN);
        types.insert("number", DataType::NUMBER);
        types.insert("oid", DataType::OID);
        types.insert("positiveInt", DataType::POSITIVEINT);
        types.insert("string", DataType::STRING);
        types.insert("time", DataType::TIME);
        types.insert("unsignedInt", DataType::UNSIGNEDINT);
        types.insert("uri", DataType::URI);
        types.insert("url", DataType::URL);
        types.insert("uuid", DataType::UUID);
        types.insert("Address", DataType::ADDRESS);
        types.insert("Age", DataType::AGE);
        types.insert("Annotation", DataType::ANNOTATION);
        types.insert("Attachment", DataType::ATTACHMENT);
        types.insert("CodeableConcept", DataType::CODEABLECONCEPT);
        types.insert("Coding", DataType::CODING);
        types.insert("ContactPoint", DataType::CONTACTPOINT);
        types.insert("Count", DataType::COUNT);
        types.insert("Distance", DataType::DISTANCE);
        types.insert("Duration", DataType::DURATION);
        types.insert("HumanName", DataType::HUMANNAME);
        types.insert("Identifier", DataType::IDENTIFIER);
        types.insert("Money", DataType::MONEY);
        types.insert("Period", DataType::PERIOD);
        types.insert("Quantity", DataType::QUANTITY);
        types.insert("Range", DataType::RANGE);
        types.insert("Ratio", DataType::RATIO);
        types.insert("Reference", DataType::REFERENCE);
        types.insert("SampledData", DataType::SAMPLEDDATA);
        types.insert("Signature", DataType::SIGNATURE);
        types.insert("Timing", DataType::TIMING);
        types.insert("ContactDetail", DataType::CONTACTDETAIL);
        types.insert("Contributor", DataType::CONTRIBUTOR);
        types.insert("DataRequirement", DataType::DATAREQUIREMENT);
        types.insert("Expression", DataType::EXPRESSION);
        types.insert("ParameterDefinition", DataType::PARAMETERDEFINITION);
        types.insert("RelatedArtifact", DataType::RELATEDARTIFACT);
        types.insert("TriggerDefinition", DataType::TRIGGERDEFINITION);
        types.insert("UsageContext", DataType::USAGECONTEXT);
        types.insert("Dosage", DataType::DOSAGE);
        types.insert("Meta", DataType::META);
        types.insert("Extension", DataType::EXTENSION);
        types.insert("Element", DataType::ELEMENT);
        types.insert("xhtml", DataType::STRING);
        types
    };
}