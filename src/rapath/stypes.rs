use std::fmt::Display;
use serde_json::ser::Formatter;
use rawbson::elem::Element;
use chrono::{DateTime, Utc, NaiveTime};
use crate::parser::ParseError;
use crate::rapath::stypes::N::{Integer, Decimal};

#[derive(Debug)]
pub enum SystemType<'a> {
    Boolean(bool),
    String(String),
    Number(SystemNumber),
    DateTime(SystemDateTime),
    Time(SystemTime),
    Quantity(SystemQuantity),
    Element(Element<'a>),
    Collection(Collection<SystemType<'a>>)
}

#[derive(Debug)]
pub enum SystemTypeType {
    Boolean,
    String,
    Number,
    DateTime,
    Time,
    Quantity,
    Element,
    Collection
}

#[derive(Debug)]
pub struct SystemNumber {
    val: N
}

#[derive(Debug)]
enum N {
    Integer(i64),
    Decimal(f64),
    // UnsignedInt(u64),
    // PositiveInt(u64),
}

#[derive(Debug)]
pub struct SystemDateTime {
    pub val: DateTime<Utc>
}

#[derive(Debug)]
pub struct SystemTime {
    pub val: NaiveTime
}

#[derive(Debug)]
pub struct SystemConstant {
    pub val: String
}

#[derive(Debug)]
pub struct SystemQuantity {
    pub val: f64,
    pub unit: String
}

#[derive(Debug)]
pub struct Collection<T> {
    val: Vec<T>
}

impl<T> Collection<T> {
    pub fn new() -> Self {
        Collection{val: vec![]}
    }
}

impl N {
}

impl SystemNumber {
    pub fn new_decimal(f: f64) -> Self {
        SystemNumber{val: N::Decimal(f)}
    }

    pub fn new_integer(i: i64) -> Self {
        SystemNumber{val: N::Integer(i)}
    }

    pub fn from(s: &String) -> Result<SystemNumber, ParseError> {
        let n = s.parse::<f64>();
        if let Err(e) = n {
            return Err(ParseError{msg: format!("{}", e)});
        }
        let n = n.unwrap();
        let sd: SystemNumber;
        if let None = s.find(".") {
            sd = SystemNumber {val: Integer(n as i64)};
        }
        else {
            sd = SystemNumber {val: Decimal(n)};
        }

        Ok(sd)
    }

    pub fn to_negative_val(&self) -> SystemNumber {
        match self.val {
            N::Integer(i) => {
                SystemNumber{val: N::Integer(-i)}
            },
            N::Decimal(d) => {
                SystemNumber{val: N::Decimal(-d)}
            }
        }
    }

    pub fn get_as_number(&self) -> Option<i64> {
        match self.val {
            N::Integer(i) => {
                Some(i)
            },
            N::Decimal(d) => {
                Some(d as i64)
            }
        }
    }
}

impl Display for SystemNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.val.to_string().as_str())
    }
}

impl Display for N {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Integer(i) => {
                f.write_str(i.to_string().as_str())
            },
            Decimal(d) => {
                f.write_str(d.to_string().as_str())
            }
        }
    }
}

impl<'a> SystemType<'a> {
    pub fn get_type(&self) -> SystemTypeType {
        use self::SystemType::*;
        match self {
            Boolean(_) => SystemTypeType::Boolean,
            String(_) => SystemTypeType::String,
            Number(_) => SystemTypeType::Number,
            DateTime(_) => SystemTypeType::DateTime,
            Time(_) => SystemTypeType::Time,
            Quantity(_) => SystemTypeType::Quantity,
            Element(_) => SystemTypeType::Element,
            Collection(_) => SystemTypeType::Collection
        }
    }

    pub fn get_as_number(&self) -> Option<i64> {
        match self {
            SystemType::Number(n) => {
                n.get_as_number()
            },
            _ => {
                None
            }
        }
    }

    pub fn get_as_string(&self) -> Option<String> {
        match self {
            SystemType::String(s) => {
                Some(s.clone())
            },
            SystemType::Boolean(b) => {
                Some(b.to_string())
            },
            SystemType::Number(n) => {
                Some(n.to_string())
            },
            _ => {
                None
            }
        }
    }
}