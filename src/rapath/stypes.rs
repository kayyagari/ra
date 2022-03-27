use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::{Display, format};
use std::ops::Add;
use std::rc::Rc;

use chrono::{DateTime, Duration, NaiveTime, Timelike, Utc};
use log::warn;
use rawbson::elem::Element;
use serde_json::ser::Formatter;
use unicase::UniCase;

use crate::errors::{EvalError, ParseError};
use crate::rapath::stypes::N::{Decimal, Integer};
use crate::rapath::{element_utils, EvalResult};
use crate::rapath::scanner::CALENDAR_UNIT_ALIAS;

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

#[derive(Debug, Clone, Eq, PartialOrd, PartialEq)]
pub struct SystemDateTime {
    val: DateTime<Utc>,
    precision: u8
}

impl SystemDateTime {
    pub fn new(val: DateTime<Utc>, precision: u8) -> Self {
        SystemDateTime{val, precision}
    }

    pub fn format(&self, fmt: &str) -> String {
        self.val.format(fmt).to_string()
    }

    pub fn millis(&self) -> i64 {
        self.val.timestamp_millis()
    }

    #[inline]
    pub fn equals<'b>(lhs: &SystemDateTime, rhs: &SystemDateTime) -> SystemType<'b> {
        if lhs.precision != rhs.precision {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val == rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn equiv(lhs: &SystemDateTime, rhs: &SystemDateTime) -> bool {
        // no check on precision while evaluating equivalence
        lhs.val == rhs.val
    }

    #[inline]
    pub fn gt<'b>(lhs: &SystemDateTime, rhs: &SystemDateTime) -> SystemType<'b> {
        if lhs.precision != rhs.precision {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val > rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn ge<'b>(lhs: &SystemDateTime, rhs: &SystemDateTime) -> SystemType<'b> {
        if lhs.precision != rhs.precision {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val >= rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn lt<'b>(lhs: &SystemDateTime, rhs: &SystemDateTime) -> SystemType<'b> {
        if lhs.precision != rhs.precision {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val < rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn le<'b>(lhs: &SystemDateTime, rhs: &SystemDateTime) -> SystemType<'b> {
        if lhs.precision != rhs.precision {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val <= rhs.val;
        SystemType::Boolean(b)
    }
}

#[derive(Debug, Clone, Eq, PartialOrd, PartialEq)]
pub struct SystemTime {
    val: NaiveTime,
    precision: u8
}

impl SystemTime {
    pub fn new(val: NaiveTime, precision: u8) -> Self {
        SystemTime{val, precision}
    }

    pub fn format(&self, fmt: &str) -> String {
        self.val.format(fmt).to_string()
    }

    pub fn millis(&self) -> i64 {
        let hmillis = self.val.hour() as i64 * 60 * 60 * 1000;
        let minmillis = self.val.minute() as i64 * 60 * 1000;
        let secmillis = self.val.second() as i64 * 1000;
        let millis = self.val.nanosecond() as i64 / 1_000_000;
        hmillis + minmillis + secmillis + millis
    }

    pub fn equals<'b>(lhs: &SystemTime, rhs: &SystemTime) -> SystemType<'b> {
        if lhs.precision != rhs.precision {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val == rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn equiv(lhs: &SystemTime, rhs: &SystemTime) -> bool {
        // no check on precision while evaluating equivalence
        lhs.val == rhs.val
    }

    #[inline]
    pub fn gt<'b>(lhs: &SystemTime, rhs: &SystemTime) -> SystemType<'b> {
        if lhs.precision != rhs.precision {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val > rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn ge<'b>(lhs: &SystemTime, rhs: &SystemTime) -> SystemType<'b> {
        if lhs.precision != rhs.precision {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val >= rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn lt<'b>(lhs: &SystemTime, rhs: &SystemTime) -> SystemType<'b> {
        if lhs.precision != rhs.precision {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val < rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn le<'b>(lhs: &SystemTime, rhs: &SystemTime) -> SystemType<'b> {
        if lhs.precision != rhs.precision {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val <= rhs.val;
        SystemType::Boolean(b)
    }
}

#[derive(Debug, Eq, PartialOrd, PartialEq)]
pub struct SystemConstant {
    val: String
}

#[derive(Debug)]
pub struct SystemQuantity {
    val: f64,
    unit: String,
    cal_unit: bool
}

impl SystemQuantity {
    pub fn new(val: f64, mut unit: String) -> Self {
        let unit_str = unit.as_str();
        let cal_unit = CALENDAR_UNIT_ALIAS.contains_key(unit_str);
        if cal_unit && unit_str == "s" {
            // because 1 second == 1 's'
            // this makes it easy to compare in equals() method
            unit = String::from("second");
        }
        SystemQuantity{val, unit, cal_unit}
    }

    pub fn equals<'b>(lhs: &SystemQuantity, rhs: &SystemQuantity) -> SystemType<'b> {
        let b = lhs.unit == rhs.unit && lhs.val == rhs.val;
        SystemType::Boolean(b)
    }

    pub fn equiv(lhs: &SystemQuantity, rhs: &SystemQuantity) -> bool {
        let mut b = false;
        if lhs.cal_unit {
            let lunit = *CALENDAR_UNIT_ALIAS.get(lhs.unit.as_str()).unwrap();
            let runit = *CALENDAR_UNIT_ALIAS.get(rhs.unit.as_str()).unwrap();
            b = lunit == runit && lhs.val == rhs.val;
        }
        else {
            b = lhs.unit == rhs.unit && lhs.val == rhs.val;
        }

        b
    }

    #[inline]
    pub fn gt<'b>(lhs: &SystemQuantity, rhs: &SystemQuantity) -> SystemType<'b> {
        if lhs.unit != rhs.unit {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val > rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn ge<'b>(lhs: &SystemQuantity, rhs: &SystemQuantity) -> SystemType<'b> {
        if lhs.unit != rhs.unit {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val >= rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn lt<'b>(lhs: &SystemQuantity, rhs: &SystemQuantity) -> SystemType<'b> {
        if lhs.unit != rhs.unit {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val < rhs.val;
        SystemType::Boolean(b)
    }

    #[inline]
    pub fn le<'b>(lhs: &SystemQuantity, rhs: &SystemQuantity) -> SystemType<'b> {
        if lhs.unit != rhs.unit {
            return SystemType::Collection(Collection::new_empty());
        }
        let b = lhs.val <= rhs.val;
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

    #[inline]
    pub fn equiv(lhs: &SystemString, rhs: &SystemString) -> bool {
        let lhs: UniCase<&str> = UniCase::from(lhs.as_str());
        let rhs: UniCase<&str> = UniCase::from(rhs.as_str());

        lhs == rhs
    }

    #[inline]
    pub fn gt(lhs: &SystemString, rhs: &SystemString) -> bool {
        lhs.as_str() > rhs.as_str()
    }

    #[inline]
    pub fn ge(lhs: &SystemString, rhs: &SystemString) -> bool {
        lhs.as_str() >= rhs.as_str()
    }

    #[inline]
    pub fn lt(lhs: &SystemString, rhs: &SystemString) -> bool {
        lhs.as_str() < rhs.as_str()
    }

    #[inline]
    pub fn le(lhs: &SystemString, rhs: &SystemString) -> bool {
        lhs.as_str() <= rhs.as_str()
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

    #[inline]
    pub fn get(&self, index: usize) -> Option<Rc<SystemType<'b>>> {
        if let Some(v) = &self.val {
            return Some(Rc::clone(&v[index]));
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

    pub fn equals(mut lhs: Rc<SystemType<'b>>, mut rhs: Rc<SystemType<'b>>) -> EvalResult<'b> {
        //println!("lhs = {}, rhs = {}", self.get_type(), other.get_type());

        if lhs.is_empty() {
            return Ok(lhs);
        }
        else if rhs.is_empty() {
            return Ok(rhs);
        }

        let ltype = lhs.get_type();
        let rtype = rhs.get_type();
        if ltype == SystemTypeType::Collection && rtype != SystemTypeType::Collection {
            lhs = SystemType::unpack_singleton_base(lhs, true)?;
        }
        else if ltype != SystemTypeType::Collection && rtype == SystemTypeType::Collection {
            rhs = SystemType::unpack_singleton_base(rhs, false)?;
        }

        let lhs = lhs.borrow();
        let rhs = rhs.borrow();

        match lhs {
            SystemType::Boolean(b1) => {
                if let SystemType::Boolean(b2) = rhs {
                    return Ok(Rc::new(SystemType::Boolean(*b1 == *b2)));
                }
            },
            SystemType::String(s1) => {
                if let SystemType::String(s2) = rhs {
                    let b = *s1 == *s2;
                    return Ok(Rc::new(SystemType::Boolean(b)));
                }
            },
            SystemType::DateTime(dt1) => {
                if let SystemType::DateTime(dt2) = rhs {
                    return Ok(Rc::new(SystemDateTime::equals(dt1, dt2)));
                }
            },
            SystemType::Time(t1) => {
                if let SystemType::Time(t2) = rhs {
                    return Ok(Rc::new(SystemTime::equals(t1, t2)));
                }
            },
            SystemType::Number(n1) => {
                if let SystemType::Number(n2) = rhs {
                    let b = *n1 == *n2;
                    return Ok(Rc::new(SystemType::Boolean(b)));
                }
            },
            SystemType::Quantity(q1) => {
                if let SystemType::Quantity(q2) = rhs {
                    return Ok(Rc::new(SystemQuantity::equals(q1,q2)));
                }
            },
            SystemType::Element(e1) => {
                if let SystemType::Element(e2) = rhs {
                    let b = element_utils::equals(e1, e2)?;
                    return Ok(Rc::new(SystemType::Boolean(b)));
                }
            },
            SystemType::Collection(c1) => {
                if let SystemType::Collection(c2) = rhs {
                    if c1.len() != c2.len() {
                        return Ok(Rc::new(SystemType::Boolean(false)));
                    }
                    for (i, lst) in c1.iter().enumerate() {
                        let rst = c2.val.as_ref().unwrap().get(i);
                        if let Some(rst) = rst {
                            let b = SystemType::equals(Rc::clone(lst), Rc::clone(rst))?;
                            if !b.is_truthy() {
                                return Ok(b);
                            }
                        }
                        else {
                            return Ok(Rc::new(SystemType::Boolean(false)));
                        }
                    }

                    return Ok(Rc::new(SystemType::Boolean(true)));
                }
            }
        }

        Ok(Rc::new(SystemType::Collection(Collection::new_empty())))
    }

    #[inline]
    fn unpack_singleton_base(base: Rc<SystemType<'b>>, is_lhs: bool) -> EvalResult<'b> {
        if let SystemType::Collection(c) = base.borrow() {
            if let Some(v) = c.get_if_singleton() {
                return Ok(v);
            }
        }

        let orientation = if is_lhs { "lhs" } else { "rhs" };
        Err(EvalError::new(format!("cannot compare, {} is not a singleton, use where function instead", orientation)))
    }

    pub fn not_equals(lhs: Rc<SystemType<'b>>, rhs: Rc<SystemType<'b>>) -> EvalResult<'b> {
        let r = SystemType::equals(lhs, rhs)?;
        if r.is_empty() {
            return Ok(r);
        }

        let b = r.as_bool().unwrap();
        Ok(Rc::new(SystemType::Boolean(!b)))
    }

    pub fn equiv(mut lhs: Rc<SystemType<'b>>, mut rhs: Rc<SystemType<'b>>) -> EvalResult<'b> {
        //println!("lhs = {}, rhs = {}", self.get_type(), other.get_type());

        let le = lhs.is_empty();
        let re = rhs.is_empty();
        if le && re {
            return Ok(Rc::new(SystemType::Boolean(true)));
        } else if le || re {
            return Ok(Rc::new(SystemType::Boolean(false)));
        }

        let lhs = lhs.borrow();
        let rhs = rhs.borrow();

        let mut equivalent = false;
        match lhs {
            SystemType::Boolean(b1) => {
                if let SystemType::Boolean(b2) = rhs {
                    equivalent = *b1 == *b2;
                }
            },
            SystemType::String(s1) => {
                if let SystemType::String(s2) = rhs {
                    equivalent = SystemString::equiv(s1, s2);
                }
            },
            SystemType::DateTime(dt1) => {
                if let SystemType::DateTime(dt2) = rhs {
                    equivalent = SystemDateTime::equiv(dt1, dt2);
                }
            },
            SystemType::Time(t1) => {
                if let SystemType::Time(t2) = rhs {
                    equivalent = SystemTime::equiv(t1, t2);
                }
            },
            SystemType::Number(n1) => {
                if let SystemType::Number(n2) = rhs {
                    equivalent = *n1 == *n2;
                }
            },
            SystemType::Quantity(q1) => {
                if let SystemType::Quantity(q2) = rhs {
                    equivalent = SystemQuantity::equiv(q1,q2);
                }
            },
            SystemType::Element(e1) => {
                if let SystemType::Element(e2) = rhs {
                    // even for equivalence equals() is called
                    // for two reasons
                    // 1. rawbson Arrays are not indexed
                    // 2. two Array "Element"s may never be compared through fhirpath, array is always converted into a collection
                    equivalent = element_utils::equals(e1, e2)?;
                }
            },
            SystemType::Collection(c1) => {
                if let SystemType::Collection(c2) = rhs {
                    let c2_len = c2.len();
                    if c1.len() == c2_len {
                        let c2_inner_vec = c2.val.as_ref().unwrap();
                        let expected_total_from_rhs = (c2_len * (c2_len + 1)) / 2;
                        let mut rhs_match_hits = 0; // sum of all indices (starting from 1)
                        'outer:
                        for (i, lst) in c1.iter().enumerate() {
                            let mut peer_found = false;
                            'inner:
                            for j in 0..c2_len {
                                let rst = &c2_inner_vec[j];
                                let b = SystemType::equiv(Rc::clone(lst), Rc::clone(rst))?.as_bool()?;
                                if b {
                                    rhs_match_hits += j + 1;
                                    peer_found = true;
                                    break 'inner;
                                }
                            }

                            if !peer_found {
                                break 'outer;
                            }
                        }

                        if rhs_match_hits == expected_total_from_rhs {
                            equivalent = true;
                        }
                    }
                }
            }
        }

        Ok(Rc::new(SystemType::Boolean(equivalent)))
    }

    pub fn not_equiv(mut lhs: Rc<SystemType<'b>>, mut rhs: Rc<SystemType<'b>>) -> EvalResult<'b> {
        let b = SystemType::equiv(lhs, rhs)?;
        let b = b.as_bool().unwrap();
        Ok(Rc::new(SystemType::Boolean(!b)))
    }

    pub fn gt(mut lhs: Rc<SystemType<'b>>, mut rhs: Rc<SystemType<'b>>) -> EvalResult<'b> {
        if lhs.is_empty() {
            return Ok(lhs);
        }
        else if rhs.is_empty() {
            return Ok(rhs);
        }

        if lhs.get_type() != rhs.get_type() {
            return Err(EvalError::new(format!("cannot apply > on incompatible types {} and {}", lhs.get_type(), rhs.get_type())));
        }

        let lhs = lhs.borrow();
        let rhs = rhs.borrow();
        let mut gt= SystemType::Boolean(false);
        match lhs {
            SystemType::String(s1) => {
                if let SystemType::String(s2) = rhs {
                    gt = SystemType::Boolean(SystemString::gt(s1, s2));
                }
            },
            SystemType::Number(n1) => {
                if let SystemType::Number(n2) = rhs {
                    gt = SystemType::Boolean(n1 > n2);
                }
            },
            SystemType::Quantity(sq1) => {
                if let SystemType::Quantity(sq2) = rhs {
                    gt = SystemQuantity::gt(sq1, sq2);
                }
            }
            SystemType::Time(t1) => {
                if let SystemType::Time(t2) = rhs {
                    gt = SystemTime::gt(t1, t2);
                }
            },
            SystemType::DateTime(dt1) => {
                if let SystemType::DateTime(dt2) = rhs {
                    gt = SystemDateTime::gt(dt1, dt2);
                }
            },
            SystemType::Collection(c1) => {
                if let SystemType::Collection(c2) = rhs {
                    if c1.len() != 1 && c2.len() != 1 {
                        return Err(EvalError::new(String::from("> can only be applied on singleton collections")));
                    }

                    let lhs = c1.val.as_ref().unwrap().into_iter().next().unwrap();
                    let rhs = c2.val.as_ref().unwrap().into_iter().next().unwrap();
                    return SystemType::gt(Rc::clone(lhs), Rc::clone(rhs));
                }
            },
            st => {
                return Err(EvalError::new(format!("> cannot be applied on operands of type {}", lhs.get_type())));
            }
        }

        Ok(Rc::new(gt))
    }

    pub fn ge(mut lhs: Rc<SystemType<'b>>, mut rhs: Rc<SystemType<'b>>) -> EvalResult<'b> {
        if lhs.is_empty() {
            return Ok(lhs);
        }
        else if rhs.is_empty() {
            return Ok(rhs);
        }

        if lhs.get_type() != rhs.get_type() {
            return Err(EvalError::new(format!("cannot apply >= on incompatible types {} and {}", lhs.get_type(), rhs.get_type())));
        }

        let lhs = lhs.borrow();
        let rhs = rhs.borrow();
        let mut ge = SystemType::Boolean(false);
        match lhs {
            SystemType::String(s1) => {
                if let SystemType::String(s2) = rhs {
                    ge = SystemType::Boolean(SystemString::ge(s1, s2));
                }
            },
            SystemType::Number(n1) => {
                if let SystemType::Number(n2) = rhs {
                    ge = SystemType::Boolean(n1 >= n2);
                }
            },
            SystemType::Quantity(sq1) => {
                if let SystemType::Quantity(sq2) = rhs {
                    ge = SystemQuantity::ge(sq1, sq2);
                }
            }
            SystemType::Time(t1) => {
                if let SystemType::Time(t2) = rhs {
                    ge = SystemTime::ge(t1, t2);
                }
            },
            SystemType::DateTime(dt1) => {
                if let SystemType::DateTime(dt2) = rhs {
                    ge = SystemDateTime::ge(dt1, dt2);
                }
            },
            SystemType::Collection(c1) => {
                if let SystemType::Collection(c2) = rhs {
                    if c1.len() != 1 && c2.len() != 1 {
                        return Err(EvalError::new(String::from(">= can only be applied on singleton collections")));
                    }

                    let lhs = c1.val.as_ref().unwrap().into_iter().next().unwrap();
                    let rhs = c2.val.as_ref().unwrap().into_iter().next().unwrap();
                    return SystemType::ge(Rc::clone(lhs), Rc::clone(rhs));
                }
            },
            st => {
                return Err(EvalError::new(format!(">= cannot be applied on operands of type {}", lhs.get_type())));
            }
        }

        Ok(Rc::new(ge))
    }

    pub fn lt(mut lhs: Rc<SystemType<'b>>, mut rhs: Rc<SystemType<'b>>) -> EvalResult<'b> {
        if lhs.is_empty() {
            return Ok(lhs);
        }
        else if rhs.is_empty() {
            return Ok(rhs);
        }

        if lhs.get_type() != rhs.get_type() {
            return Err(EvalError::new(format!("cannot apply < on incompatible types {} and {}", lhs.get_type(), rhs.get_type())));
        }

        let lhs = lhs.borrow();
        let rhs = rhs.borrow();
        let mut lt = SystemType::Boolean(false);
        match lhs {
            SystemType::String(s1) => {
                if let SystemType::String(s2) = rhs {
                    lt = SystemType::Boolean(SystemString::lt(s1, s2));
                }
            },
            SystemType::Number(n1) => {
                if let SystemType::Number(n2) = rhs {
                    lt = SystemType::Boolean(n1 < n2);
                }
            },
            SystemType::Quantity(sq1) => {
                if let SystemType::Quantity(sq2) = rhs {
                    lt = SystemQuantity::lt(sq1, sq2);
                }
            }
            SystemType::Time(t1) => {
                if let SystemType::Time(t2) = rhs {
                    lt = SystemTime::lt(t1, t2);
                }
            },
            SystemType::DateTime(dt1) => {
                if let SystemType::DateTime(dt2) = rhs {
                    lt = SystemDateTime::lt(dt1, dt2);
                }
            },
            SystemType::Collection(c1) => {
                if let SystemType::Collection(c2) = rhs {
                    if c1.len() != 1 && c2.len() != 1 {
                        return Err(EvalError::new(String::from("< can only be applied on singleton collections")));
                    }

                    let lhs = c1.val.as_ref().unwrap().into_iter().next().unwrap();
                    let rhs = c2.val.as_ref().unwrap().into_iter().next().unwrap();
                    return SystemType::lt(Rc::clone(lhs), Rc::clone(rhs));
                }
            },
            st => {
                return Err(EvalError::new(format!("< cannot be applied on operands of type {}", lhs.get_type())));
            }
        }

        Ok(Rc::new(lt))
    }

    pub fn le(mut lhs: Rc<SystemType<'b>>, mut rhs: Rc<SystemType<'b>>) -> EvalResult<'b> {
        if lhs.is_empty() {
            return Ok(lhs);
        }
        else if rhs.is_empty() {
            return Ok(rhs);
        }

        if lhs.get_type() != rhs.get_type() {
            return Err(EvalError::new(format!("cannot apply <= on incompatible types {} and {}", lhs.get_type(), rhs.get_type())));
        }

        let lhs = lhs.borrow();
        let rhs = rhs.borrow();
        let mut le = SystemType::Boolean(false);
        match lhs {
            SystemType::String(s1) => {
                if let SystemType::String(s2) = rhs {
                    le = SystemType::Boolean(SystemString::le(s1, s2));
                }
            },
            SystemType::Number(n1) => {
                if let SystemType::Number(n2) = rhs {
                    le = SystemType::Boolean(n1 <= n2);
                }
            },
            SystemType::Quantity(sq1) => {
                if let SystemType::Quantity(sq2) = rhs {
                    le = SystemQuantity::le(sq1, sq2);
                }
            }
            SystemType::Time(t1) => {
                if let SystemType::Time(t2) = rhs {
                    le = SystemTime::le(t1, t2);
                }
            },
            SystemType::DateTime(dt1) => {
                if let SystemType::DateTime(dt2) = rhs {
                    le = SystemDateTime::le(dt1, dt2);
                }
            },
            SystemType::Collection(c1) => {
                if let SystemType::Collection(c2) = rhs {
                    if c1.len() != 1 && c2.len() != 1 {
                        return Err(EvalError::new(String::from("< can only be applied on singleton collections")));
                    }

                    let lhs = c1.val.as_ref().unwrap().into_iter().next().unwrap();
                    let rhs = c2.val.as_ref().unwrap().into_iter().next().unwrap();
                    return SystemType::le(Rc::clone(lhs), Rc::clone(rhs));
                }
            },
            st => {
                return Err(EvalError::new(format!("<= cannot be applied on operands of type {}", lhs.get_type())));
            }
        }

        Ok(Rc::new(le))
    }

    pub fn is(lhs: Rc<SystemType<'b>>, rhs: Rc<SystemType<'b>>) -> EvalResult<'b> {
        if lhs.is_empty() {
            return Ok(lhs);
        }

        match lhs.borrow() {
            SystemType::Collection(c1) => {
                if c1.len() != 1 {
                    return Err(EvalError::new(String::from("lhs must be a singleton for evaluating is operation")));
                }
                let lhs = c1.val.as_ref().unwrap().into_iter().next().unwrap();
                SystemType::is(Rc::clone(lhs), rhs)
            },
            SystemType::Element(e) => {
                if let SystemType::String(type_id) = rhs.borrow() {
                    let b = element_utils::is_resource_of_type(e, type_id.as_str());
                    return Ok(Rc::new(SystemType::Boolean(b)));
                }
                Err(EvalError::new(String::from("rhs is not a valid type identifier")))
            },
            _ => {
                Ok(Rc::new(SystemType::Collection(Collection::new_empty())))
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
    use std::rc::Rc;
    use anyhow::Error;
    use bson::spec::ElementType;
    use chrono::{DateTime, NaiveTime, Utc};
    use rawbson::elem::Element;
    use serde_json::Value;
    use crate::rapath::stypes::{Collection, SystemDateTime, SystemNumber, SystemQuantity, SystemString, SystemTime, SystemType};
    use crate::utils::test_utils::{read_patient, to_docbuf, update};

    #[test]
    fn test_equality() -> Result<(), Error> {
        let mut p_json = read_patient();
        let p1 = to_docbuf(&p_json);
        let p1 = Element::new(ElementType::EmbeddedDocument, p1.as_bytes());
        let p2 = to_docbuf(&p_json.clone());
        let p2 = Element::new(ElementType::EmbeddedDocument, p2.as_bytes());

        let st1 = Rc::new(SystemType::Element(p1));
        let st2 = Rc::new(SystemType::Element(p2));
        let r = SystemType::equals(Rc::clone(&st1), st2)?;
        assert_eq!(true, r.as_bool().unwrap());

        update(&mut p_json, "/name/given", Value::String(String::from("Peacock")));
        let p2 = to_docbuf(&p_json);
        let p2 = Element::new(ElementType::EmbeddedDocument, p2.as_bytes());
        let st2 = Rc::new(SystemType::Element(p2));
        let r = SystemType::equals(st1, st2)?;
        assert_eq!(false, r.as_bool().unwrap());

        Ok(())
    }

    #[test]
    fn test_system_quantity_equality() -> Result<(), Error> {
        let lhs = SystemQuantity::new(1.0, String::from("second"));
        let rhs = SystemQuantity::new(1.0, String::from("s"));
        assert!(SystemQuantity::equals(&lhs, &rhs).as_bool().unwrap());

        let lhs = SystemQuantity::new(1.0, String::from("mg"));
        let rhs = SystemQuantity::new(1.0, String::from("mg"));
        assert!(SystemQuantity::equals(&lhs, &rhs).as_bool().unwrap());

        let lhs = SystemQuantity::new(1.0, String::from("year"));
        let rhs = SystemQuantity::new(1.0, String::from("a"));
        assert_eq!(false, SystemQuantity::equals(&lhs, &rhs).as_bool().unwrap());

        Ok(())
    }

    #[test]
    fn test_compare_empty_collection() {
        let lhs = SystemType::Collection(Collection::new_empty());
        let rhs = SystemType::Collection(Collection::new_empty());
        let r = SystemType::equals(Rc::new(lhs), Rc::new(rhs)).unwrap();
        assert_eq!(true, r.is_empty());
    }

    #[test]
    fn test_equivalence() {
        let lhs = SystemQuantity::new(1.0, String::from("year"));
        let rhs = SystemQuantity::new(1.0, String::from("a"));
        assert!(SystemQuantity::equiv(&lhs, &rhs));

        let lhs = SystemQuantity::new(1.0, String::from("second"));
        let rhs = SystemQuantity::new(1.0, String::from("s"));
        assert!(SystemQuantity::equiv(&lhs, &rhs));

        let lhs = SystemString::from_slice("α is alpha, β is beta");
        let rhs = SystemString::from_slice("α is ALPHA, β is beta");
        assert!(SystemString::equiv(&lhs, &rhs));

        let lhs = SystemString::from_slice("नमस्ते");
        let rhs = SystemString::from_slice("नमस्ते");
        assert!(SystemString::equiv(&lhs, &rhs));

        let mut lhs = Collection::new();
        lhs.push(Rc::new(SystemType::Number(SystemNumber::new_integer(11))));
        lhs.push(Rc::new(SystemType::Number(SystemNumber::new_integer(9))));

        // order reversed in RHS
        let mut rhs = Collection::new();
        rhs.push(Rc::new(SystemType::Number(SystemNumber::new_integer(9))));
        rhs.push(Rc::new(SystemType::Number(SystemNumber::new_integer(11))));

        let lhs = Rc::new(SystemType::Collection(lhs));
        let rhs = Rc::new(SystemType::Collection(rhs));
        let r = SystemType::equals(Rc::clone(&lhs), Rc::clone(&rhs)).unwrap();
        assert_eq!(false, r.as_bool().unwrap());

        let r = SystemType::not_equals(Rc::clone(&lhs), Rc::clone(&rhs)).unwrap();
        assert_eq!(true, r.as_bool().unwrap());

        let r = SystemType::equiv(Rc::clone(&lhs), Rc::clone(&rhs)).unwrap();
        assert_eq!(true, r.as_bool().unwrap());

        let r = SystemType::not_equiv(Rc::clone(&lhs), Rc::clone(&rhs)).unwrap();
        assert_eq!(false, r.as_bool().unwrap());
    }

    #[test]
    fn test_comparison() {
        let mut candidates = Vec::new();
        // (SystemType, SystemType,[4; outcome])
        // outcome: i32 => 1=true, 0=false, -1=empty
        candidates.push((SystemType::String(SystemString::from_slice("abc")), SystemType::String(SystemString::from_slice("ABC")), [1, 1, 0, 0]));
        candidates.push((SystemType::Number(SystemNumber::new_integer(2)), SystemType::Number(SystemNumber::new_integer(5)), [0, 0, 1, 1]));
        candidates.push((SystemType::Collection(Collection::new_empty()), SystemType::Number(SystemNumber::new_integer(5)), [-1, -1, -1, -1]));

        let mut lhs_col = Collection::new();
        lhs_col.push(Rc::new(SystemType::Number(SystemNumber::new_integer(5))));
        let mut rhs_col = Collection::new();
        rhs_col.push(Rc::new(SystemType::Number(SystemNumber::new_integer(2))));
        candidates.push((SystemType::Collection(lhs_col), SystemType::Collection(rhs_col), [1, 1, 0, 0]));

        candidates.push((SystemType::Quantity(SystemQuantity::new(1.0, String::from("year"))), SystemType::Quantity(SystemQuantity::new(1.0, String::from("a"))), [-1, -1, -1, -1]));
        candidates.push((SystemType::Quantity(SystemQuantity::new(2.0, String::from("second"))), SystemType::Quantity(SystemQuantity::new(1.0, String::from("s"))), [1, 1, 0, 0]));

        candidates.push((SystemType::DateTime(SystemDateTime::new(Utc::now(), 63)), SystemType::DateTime(SystemDateTime::new(Utc::now(), 63)), [0, 0, 1, 1]));
        candidates.push((SystemType::DateTime(SystemDateTime::new(Utc::now(), 63)), SystemType::DateTime(SystemDateTime::new(Utc::now(), 32)), [-1, -1, -1, -1]));
        candidates.push((SystemType::Time(SystemTime::new(NaiveTime::from_hms(11, 0, 0), 7)), SystemType::Time(SystemTime::new(NaiveTime::from_hms(11, 0, 0), 7)), [0, 1, 0, 1]));

        for (lhs, rhs, outcome) in candidates {
            let comparators = [SystemType::gt, SystemType::ge, SystemType::lt, SystemType::le];
            let lhs = Rc::new(lhs);
            let rhs = Rc::new(rhs);
            for (i, f) in comparators.into_iter().enumerate() {
                println!("evaluating {} function with lhs {:?} and rhs {:?}", i+1, lhs, rhs);
                let r = f(Rc::clone(&lhs), Rc::clone(&rhs)).unwrap();
                match outcome[i] {
                    -1 => assert!(r.is_empty()),
                    1 => assert_eq!(true, r.as_bool().unwrap()),
                    0 => assert_eq!(false, r.as_bool().unwrap()),
                    _ => {
                        assert!(false, "invalid input, unknown outcome")
                    }
                }
            }
        }
    }
}