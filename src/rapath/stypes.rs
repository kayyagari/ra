use std::cmp::Ordering;
use std::fmt::Display;
use std::ops::Add;
use std::ptr::eq;
use std::rc::Rc;

use chrono::{DateTime, NaiveTime, Utc};
use rawbson::elem::Element;
use serde_json::ser::Formatter;

use crate::errors::{EvalError, ParseError};
use crate::rapath::stypes::N::{Decimal, Integer};
use crate::rapath::element_utils;

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

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
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
    val: DateTime<Utc>
}

#[derive(Debug, Eq, PartialOrd, PartialEq)]
pub struct SystemTime {
    val: NaiveTime
}

#[derive(Debug, Eq, PartialOrd, PartialEq)]
pub struct SystemConstant {
    val: String
}

#[derive(Debug)]
pub struct SystemQuantity {
    val: f64,
    unit: String
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
    val: Option<Vec<Rc<SystemType<'a>>>>
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
        self.val.as_i64()
    }

    pub fn as_f64(&self) -> f64 {
        self.val.as_f64()
    }
}

impl Display for SystemNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.val.to_string().as_str())
    }
}

impl N {
    #[inline]
    fn as_i64(&self) -> i64 {
        match self {
            N::Integer(i) => {
                *i
            },
            N::Decimal(d) => {
                *d as i64
            }
        }
    }

    #[inline]
    fn as_f64(&self) -> f64 {
        match self {
            N::Integer(i) => {
                *i as f64
            },
            N::Decimal(d) => {
                *d
            }
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

impl Eq for N {}
impl PartialEq for N {
    fn eq(&self, other: &Self) -> bool {
        let lhs = self.as_f64();
        let rhs = other.as_f64();
        lhs == rhs
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

impl Ord for N {
    fn cmp(&self, other: &Self) -> Ordering {
        let lhs = self.as_f64();
        let rhs = other.as_f64();

        if lhs == rhs {
            return Ordering::Equal;
        }
        else if lhs < rhs {
            return Ordering::Less;
        }

        Ordering::Greater
    }
}

impl PartialOrd for N {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Add for SystemNumber {
    type Output = SystemNumber;

    fn add(self, rhs: Self) -> Self::Output {
        let val = self.val + rhs.val;
        SystemNumber{val}
    }
}

impl Add for N {
    type Output = N;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            Integer(i) => {
                if let Decimal(d) = rhs {
                    return Decimal(i as f64 + d);
                }

                return Integer(i + rhs.as_i64());
            },
            Decimal(d) => {
                let other;
                if let Integer(i) = rhs {
                    other = i as f64;
                }
                else {
                    other = rhs.as_f64();
                }

                return Decimal(d + other);
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
                if let Integer(i) = n.val {
                    Ok(i)
                }
                else {
                    Err(EvalError::new(format!("{} is not an integer value", n.val)))
                }
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
        match self {
            SystemType::Boolean(b) => {
                Ok(*b)
            },
            SystemType::String(s) => {
                let s = s.as_str();
                let b = match s.to_lowercase().as_str() {
                    "true" => true,
                    "t" => true,
                    "yes" => true,
                    "y" => true,
                    "1" => true,
                    "1.0" => true,
                    "false" => false,
                    "f" => false,
                    "no" => false,
                    "n" => false,
                    "0" => false,
                    "0.0" => false,
                    _ => {
                        return Err(EvalError::new(format!("Cannot convert string {} to boolean", s)));
                    }
                };

                Ok(b)
            },
            SystemType::Number(sd) => {
                Ok(sd.as_f64() == 1.0)
            },
            SystemType::Collection(c) => {
                if c.is_empty() {
                    return Ok(false);
                }

                if c.len() > 1 {
                    return Err(EvalError::from_str("Cannot convert non-singleton collection to boolean"));
                }

                c.val.as_ref().unwrap().get(0).unwrap().as_bool()
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
            SystemType::Collection(c) => !c.is_empty(),
            _ => true // there is some value
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

        match self {
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
            SystemType::Element(e1) => {
                if let SystemType::Element(e2) = other {
                    let r = element_utils::eq(e1, e2);
                    if let Ok(r) = r {
                        return r;
                    }
                }
            },
            SystemType::Collection(c1) => {
                if let SystemType::Collection(c2) = other {
                    if c1.len() != c2.len() {
                        return false;
                    }
                    for (i, lst) in c1.iter().enumerate() {
                        let rst = c2.val.as_ref().unwrap().get(i);
                        if let Some(rst) = rst {
                            let b = lst.eq(rst);
                            if !b {
                                return b;
                            }
                        }
                        else {
                            return false;
                        }
                    }
                }
            }
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

#[cfg(test)]
mod tests {
    use bson::spec::ElementType;
    use rawbson::elem::Element;
    use serde_json::Value;
    use crate::rapath::stypes::SystemType;
    use crate::test_utils::{read_patient, to_docbuf, update};

    #[test]
    fn test_equality() {
        let mut p_json = read_patient();
        let p1 = to_docbuf(&p_json);
        let p1 = Element::new(ElementType::EmbeddedDocument, p1.as_bytes());
        let p2 = to_docbuf(&p_json.clone());
        let p2 = Element::new(ElementType::EmbeddedDocument, p2.as_bytes());

        let st1 = SystemType::Element(p1);
        let st2 = SystemType::Element(p2);
        assert_eq!(st1, st2);

        update(&mut p_json, "/name/given", Value::String(String::from("Peacock")));
        let p2 = to_docbuf(&p_json);
        let p2 = Element::new(ElementType::EmbeddedDocument, p2.as_bytes());
        let st2 = SystemType::Element(p2);
        assert_ne!(st1, st2);
    }
}