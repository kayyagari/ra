use std::cmp::Ordering;
use std::fmt::Display;
use std::ops::Add;
use std::ptr::eq;
use std::rc::Rc;

use chrono::{DateTime, NaiveTime, Utc};
use log::warn;
use rawbson::elem::Element;
use serde_json::ser::Formatter;

use crate::errors::{EvalError, ParseError};
use crate::rapath::stypes::N::{Decimal, Integer};
use crate::rapath::element_utils;

#[derive(Debug)]
pub enum SystemType<'b> {
    Boolean(bool),
    String(SystemString<'b>),
    Number(SystemNumber),
    DateTime(SystemDateTime),
    Time(SystemTime),
    Quantity(SystemQuantity),
    Element(Element<'b>),
    Collection(Collection<'b>)
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

impl SystemDateTime {
    pub fn new(val: DateTime<Utc>) -> Self {
        SystemDateTime{val}
    }

    pub fn equals<'b>(lhs: &SystemDateTime, rhs: &SystemDateTime) -> SystemType<'b> {
        let b = lhs.val.eq(&rhs.val);
        SystemType::Boolean(b)
    }
}

#[derive(Debug, Eq, PartialOrd, PartialEq)]
pub struct SystemTime {
    val: NaiveTime
}

impl SystemTime {
    pub fn new(val: NaiveTime) -> Self {
        SystemTime{val}
    }
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

impl SystemQuantity {
    pub fn new(val: f64, unit: String) -> Self {
        SystemQuantity{val, unit}
    }

    pub fn equals<'b>(lhs: &SystemQuantity, rhs: &SystemQuantity) -> SystemType<'b> {
        let b = lhs.unit == rhs.unit && lhs.val == rhs.val;
        SystemType::Boolean(b)
    }
}

#[derive(Debug)]
pub struct SystemString<'b> {
    owned: Option<String>, // TODO use SmartString to minimize allocations on heap
    borrowed: Option<&'b str>
}

impl<'b> SystemString<'b> {
    #[inline]
    pub fn new(s: String) -> Self {
        SystemString{owned: Some(s), borrowed: None}
    }

    #[inline]
    pub fn from_slice(s: &'b str) -> Self {
        SystemString{owned: None, borrowed: Some(s)}
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        if let Some(s) = self.borrowed {
            return s;
        }

        self.owned.as_ref().unwrap().as_str()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.as_str().len()
    }
}

impl<'b> Eq for SystemString<'b>{}
impl<'b> PartialEq for SystemString<'b> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

#[derive(Debug)]
pub struct Collection<'b> {
    val: Option<Vec<Rc<SystemType<'b>>>>
}

impl<'b> Collection<'b> {
    #[inline]
    pub fn new() -> Self {
        Collection{val: Option::Some(vec!())}
    }

    #[inline]
    pub fn new_empty() -> Self {
        Collection{val: Option::None}
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        if let Some(v) = &self.val {
            return v.is_empty()
        }

        true
    }

    #[inline]
    pub fn push(&mut self, st: Rc<SystemType<'b>>) {
        self.val.as_mut().unwrap().push(st);
    }

    #[inline]
    pub fn iter(&self) -> core::slice::Iter<Rc<SystemType<'b>>> {
         self.val.as_ref().unwrap().iter()
    }

    #[inline]
    pub fn len(&self) -> usize {
        if let Some(v) = &self.val {
            return v.len();
        }
        0
    }

    #[inline]
    pub fn get_if_singleton(&self) -> Option<Rc<SystemType<'b>>> {
        if let Some(v) = &self.val {
            if v.len() == 1 {
                return Some(Rc::clone(&v[0]));
            }
        }

        None
    }
}

impl SystemNumber {
    #[inline]
    pub fn new_decimal(f: f64) -> Self {
        SystemNumber{val: N::Decimal(f)}
    }

    #[inline]
    pub fn new_integer(i: i64) -> Self {
        SystemNumber{val: N::Integer(i)}
    }

    #[inline]
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

    #[inline]
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

    #[inline]
    pub fn as_i64(&self) -> i64 {
        self.val.as_i64()
    }

    #[inline]
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

impl<'b> SystemType<'b> {
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

    pub fn equals(lhs: &SystemType, rhs: &SystemType) -> SystemType<'b> {
        //println!("lhs = {}, rhs = {}", self.get_type(), other.get_type());
        if lhs.get_type() != rhs.get_type() {
            return SystemType::Collection(Collection::new_empty());
        }

        match lhs {
            SystemType::Boolean(b1) => {
                if let SystemType::Boolean(b2) = rhs {
                    return SystemType::Boolean(*b1 == *b2);
                }
            },
            SystemType::String(s1) => {
                if let SystemType::String(s2) = rhs {
                    let b = *s1 == *s2;
                    return SystemType::Boolean(b);
                }
            },
            SystemType::DateTime(dt1) => {
                if let SystemType::DateTime(dt2) = rhs {
                    return SystemDateTime::equals(dt1, dt2);
                }
            },
            SystemType::Number(n1) => {
                if let SystemType::Number(n2) = rhs {
                    let b = *n1 == *n2;
                    return SystemType::Boolean(b);
                }
            },
            SystemType::Quantity(q1) => {
                if let SystemType::Quantity(q2) = rhs {
                    return SystemQuantity::equals(q1,q2);
                }
            },
            SystemType::Element(e1) => {
                if let SystemType::Element(e2) = rhs {
                    let b = element_utils::eq(e1, e2);
                    if let Err(b) = b {
                        warn!("error while comparing equality on two Elements, undefined will be returned. {}", b.to_string());
                    }
                    else {
                        return SystemType::Boolean(b.unwrap());
                    }
                }
            },
            SystemType::Collection(c1) => {
                if let SystemType::Collection(c2) = rhs {
                    if c1.len() != c2.len() {
                        return SystemType::Boolean(false);
                    }
                    for (i, lst) in c1.iter().enumerate() {
                        let rst = c2.val.as_ref().unwrap().get(i);
                        if let Some(rst) = rst {
                            let b = lst.eq(rst);
                            if !b {
                                return SystemType::Boolean(b);
                            }
                        }
                        else {
                            return SystemType::Boolean(false);
                        }
                    }
                }
            },
            _ => {
            }
        }

        SystemType::Collection(Collection::new_empty())
    }

    pub fn not_equals(lhs: &SystemType, rhs: &SystemType) -> SystemType<'b> {
        let r = SystemType::equals(lhs, rhs);
        if r.is_empty() {
            return r;
        }

        let b = r.as_bool().unwrap();
        SystemType::Boolean(!b)
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

impl<'b> Eq for SystemType<'b> {}
impl<'b> PartialEq for SystemType<'b> {
    fn eq(&self, other: &Self) -> bool {
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
    use crate::rapath::stypes::{Collection, SystemType};
    use crate::utils::test_utils::{read_patient, to_docbuf, update};

    #[test]
    fn test_equality() {
        let mut p_json = read_patient();
        let p1 = to_docbuf(&p_json);
        let p1 = Element::new(ElementType::EmbeddedDocument, p1.as_bytes());
        let p2 = to_docbuf(&p_json.clone());
        let p2 = Element::new(ElementType::EmbeddedDocument, p2.as_bytes());

        let st1 = SystemType::Element(p1);
        let st2 = SystemType::Element(p2);
        let r = SystemType::equals(&st1, &st2);
        assert_eq!(true, r.as_bool().unwrap());

        update(&mut p_json, "/name/given", Value::String(String::from("Peacock")));
        let p2 = to_docbuf(&p_json);
        let p2 = Element::new(ElementType::EmbeddedDocument, p2.as_bytes());
        let st2 = SystemType::Element(p2);
        let r = SystemType::equals(&st1, &st2);
        assert_eq!(false, r.as_bool().unwrap());
    }
}