use std::fmt::Display;
use std::ptr::eq;
use std::rc::Rc;

use chrono::{DateTime, NaiveTime, Utc};
use rawbson::elem::Element;
use serde_json::ser::Formatter;

use crate::errors::{EvalError, ParseError};
use crate::rapath::stypes::N::{Decimal, Integer};

#[derive(Debug)]
pub enum SystemType<'a> {
    Boolean(bool),
    String(SystemString<'a>),
    Number(SystemNumber),
    DateTime(SystemDateTime),
    Time(SystemTime),
    Quantity(SystemQuantity),
    Element(Element<'a>),
    Collection(Collection<'a>)
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
pub struct SystemString<'a> {
    owned: Option<String>, // TODO use SmartString to minimize allocations on heap
    borrowed: Option<&'a str>
}

impl<'a> SystemString<'a> {
    pub fn new(s: String) -> Self {
        SystemString{owned: Some(s), borrowed: None}
    }

    pub fn from_slice(s: &'a str) -> Self {
        SystemString{owned: None, borrowed: Some(s)}
    }

    pub fn as_str(&self) -> &str {
        if let Some(s) = self.borrowed {
            return s;
        }

        self.owned.as_ref().unwrap().as_str()
    }

    pub fn len(&self) -> usize {
        self.as_str().len()
    }
}

impl<'a> Eq for SystemString<'a>{}
impl<'a> PartialEq for SystemString<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }

    fn ne(&self, other: &Self) -> bool {
        self.as_str() != other.as_str()
    }
}

#[derive(Debug)]
pub struct Collection<'a> {
    pub val: Option<Vec<Rc<SystemType<'a>>>>
}

impl<'a> Collection<'a> {
    pub fn new() -> Self {
        Collection{val: Option::Some(vec!())}
    }

    pub fn new_empty() -> Self {
        Collection{val: Option::None}
    }

    pub fn is_empty(&self) -> bool {
        if let Some(v) = &self.val {
            return v.is_empty()
        }

        true
    }

    pub fn push(&mut self, st: Rc<SystemType<'a>>) {
        self.val.as_mut().unwrap().push(st);
    }

    pub fn iter(&self) -> core::slice::Iter<Rc<SystemType<'a>>> {
         self.val.as_ref().unwrap().iter()
    }

    pub fn len(&self) -> usize {
        if let Some(v) = &self.val {
            return v.len();
        }
        0
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

impl Clone for SystemNumber {
    fn clone(&self) -> Self {
        SystemNumber{val: self.val.clone()}
    }
}

impl Clone for N {
    fn clone(&self) -> Self {
        match self {
            Integer(i) => Integer(*i),
            Decimal(d) => Decimal(*d)
        }
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

impl Display for SystemTypeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use self::SystemTypeType::*;
        let name = match self {
            Boolean=> "Boolean",
            String=> "String",
            Number=> "Number",
            DateTime=> "DateTime",
            Time=> "Time",
            Quantity=> "Quantity",
            Element=> "Element",
            Collection=> "Collection"
        };

        f.write_str(name)
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

    pub fn as_i64(&self) -> Result<i64, EvalError> {
        match self {
            SystemType::Number(n) => {
                Ok(n.as_i64())
            },
            st => {
                Err(EvalError::new(format!("Cannot convert type {} to integer", st.get_type())))
            }
        }
    }

    pub fn as_f64(&self) -> Result<f64, EvalError> {
        match self {
            SystemType::Number(n) => {
                Ok(n.as_f64())
            },
            st => {
                Err(EvalError::new(format!("Cannot convert type {} to decimal", st.get_type())))
            }
        }
    }

    pub fn as_string(&self) -> Result<String, EvalError> {
        match self {
            SystemType::String(s) => {
                Ok(String::from(s.as_str()))
            },
            SystemType::Boolean(b) => {
                Ok(b.to_string())
            },
            SystemType::Number(n) => {
                Ok(n.to_string())
            },
            st => {
                Err(EvalError::new(format!("Cannot convert type {} to String", st.get_type())))
            }
        }
    }

    pub fn as_bool(&self) -> Result<bool, EvalError> {
        match &*self {
            SystemType::Boolean(b) => {
                Ok(*b)
            },
            SystemType::String(s) => {
                let s = s.as_str();
                let b = s.parse::<bool>();
                if let Ok(b) = b {
                    return Ok(b);
                }

                return Err(EvalError::new(format!("Cannot convert string {} to boolean", s)));
            },
            SystemType::Number(sd) => {
                Ok(sd.as_i64() > 0)
            },
            st => {
                Err(EvalError::new(format!("Cannot convert type {} to boolean", st.get_type())))
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            SystemType::Collection(c) => c.is_empty(),
            _ => false
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            SystemType::Boolean(b) => *b,
            SystemType::String(s) => s.len() > 0,
            SystemType::Collection(c) => !c.is_empty(),
            _ => true // there is some value
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

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}
