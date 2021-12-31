use std::fmt::Display;
use serde_json::ser::Formatter;
use rawbson::elem::Element;
use chrono::{DateTime, Utc, NaiveTime};
use crate::errors::ParseError;
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

#[derive(Debug, Eq, PartialEq)]
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

#[derive(Debug, Eq, PartialOrd, PartialEq)]
pub struct SystemDateTime {
    pub val: DateTime<Utc>
}

#[derive(Debug, Eq, PartialOrd, PartialEq)]
pub struct SystemTime {
    pub val: NaiveTime
}

#[derive(Debug, Eq, PartialOrd, PartialEq)]
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
            return Err(ParseError::new(format!("{}", e)));
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

    pub fn as_i64(&self) -> i64 {
        match self.val {
            N::Integer(i) => {
                i
            },
            N::Decimal(d) => {
                d as i64
            }
        }
    }

    pub fn as_f64(&self) -> f64 {
        match self.val {
            N::Integer(i) => {
                i as f64
            },
            N::Decimal(d) => {
                d
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
                Some(n.as_i64())
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

    pub fn as_bool(&self) -> Option<bool> {
        match &*self {
            SystemType::Boolean(b) => {
                Some(*b)
            },
            SystemType::String(s) => {
                let b = s.parse::<bool>();
                if b.is_ok() {
                    let b = b.unwrap();
                    return Some(b);
                }

                return Some(false);
            },
            SystemType::Number(sd) => {
                Some(sd.as_i64() > 0)
            },
            _ => {
                None
            }
        }
    }
}

impl Eq for SystemNumber {}
impl PartialEq for SystemNumber {
    fn eq(&self, other: &Self) -> bool {
        match self.val {
            N::Integer(i1) => {
                i1 == other.as_i64()
            },
            N::Decimal(d1) => {
                d1 == other.as_f64()
            }
        }
    }

    fn ne(&self, other: &Self) -> bool {
        match self.val {
            N::Integer(i1) => {
                i1 != other.as_i64()
            },
            N::Decimal(d1) => {
                d1 != other.as_f64()
            }
        }
    }
}

impl Eq for SystemQuantity {}
impl PartialEq for SystemQuantity {
    fn eq(&self, other: &Self) -> bool {
        self.unit == other.unit && self.val == other.val
    }

    fn ne(&self, other: &Self) -> bool {
        self.unit != other.unit || self.val != other.val
    }
}

impl<'a> Eq for SystemType<'a> {}
impl<'a> PartialEq for SystemType<'a> {
    fn eq(&self, other: &Self) -> bool {
        if self.get_type() != other.get_type() {
            return false;
        }

        match &*self {
            SystemType::Boolean(b1) => {
                if let SystemType::Boolean(b2) = other {
                    return *b1 == *b2;
                }
            },
            SystemType::String(s1) => {
                if let SystemType::String(s2) = other {
                    return *s1 == *s2;
                }
            },
            SystemType::DateTime(dt1) => {
                if let SystemType::DateTime(dt2) = other {
                    return *dt1 == *dt2;
                }
            },
            SystemType::Number(n1) => {
                if let SystemType::Number(n2) = other {
                    return *n1 == *n2;
                }
            },
            SystemType::Quantity(q1) => {
                if let SystemType::Quantity(q2) = other {
                    return *q1 == *q2;
                }
            },
            _ => {
                return false;
            }
        }

        false
    }

    // the eq()'s code is duplicated and changed the operator and default return value
    fn ne(&self, other: &Self) -> bool {
        if self.get_type() != other.get_type() {
            return true;
        }

        match &*self {
            SystemType::Boolean(b1) => {
                if let SystemType::Boolean(b2) = other {
                    return *b1 != *b2;
                }
            },
            SystemType::String(s1) => {
                if let SystemType::String(s2) = other {
                    return *s1 != *s2;
                }
            },
            SystemType::DateTime(dt1) => {
                if let SystemType::DateTime(dt2) = other {
                    return *dt1 != *dt2;
                }
            },
            SystemType::Number(n1) => {
                if let SystemType::Number(n2) = other {
                    return *n1 != *n2;
                }
            },
            SystemType::Quantity(q1) => {
                if let SystemType::Quantity(q2) = other {
                    return *q1 != *q2;
                }
            },
            _ => {
                return true;
            }
        }

        true
    }
}
