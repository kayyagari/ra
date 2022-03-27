use std::collections::{HashMap, VecDeque};
use thiserror::Error;
use std::fmt::{Display, Formatter, Write};
use std::iter::Peekable;
use std::str::CharIndices;
use chrono::prelude::*;
use chrono::format::Fixed::TimezoneOffset;
use chrono::format::Parsed;

use lazy_static::lazy_static;
use regex::Regex;

use crate::errors::ScanError;
use crate::rapath::stypes::{SystemDateTime, SystemTime};

use self::Token::*;

const TIME_FORMAT: &'static str = "@T%H:%M:%S%.3f";
const DATETIME_FORMAT: &'static str = "@%Y-%m-%dT%H:%M:%S%.3fZ";

lazy_static! {
 pub static ref KEYWORDS: HashMap<&'static str, Token> = {
        let mut words = HashMap::new();
        words.insert("div", Token::DIV);
        words.insert("mod", Token::MOD);
        words.insert("is", Token::IS);
        words.insert("as", Token::AS);
        words.insert("in", Token::IN);
        words.insert("contains", Token::CONTAINS);
        words.insert("and", Token::AND);
        words.insert("or", Token::OR);
        words.insert("xor", Token::XOR);
        words.insert("implies", Token::IMPLIES);
        words.insert("true", Token::TRUE);
        words.insert("false", Token::FALSE);
        words.insert("$this", Token::DOLLAR_THIS);
        words.insert("$index", Token::DOLLAR_INDEX);
        words.insert("$total", Token::DOLLAR_TOTAL);

        words.insert("day", Token::DAY);
        words.insert("days", Token::DAYS);
        words.insert("hour", Token::HOUR);
        words.insert("hours", Token::HOURS);
        words.insert("millisecond", Token::MILLISECOND);
        words.insert("milliseconds", Token::MILLISECONDS);
        words.insert("minute", Token::MINUTE);
        words.insert("minutes", Token::MINUTES);
        words.insert("month", Token::MONTH);
        words.insert("months", Token::MONTHS);
        words.insert("second", Token::SECOND);
        words.insert("seconds", Token::SECONDS);
        words.insert("week", Token::WEEK);
        words.insert("weeks", Token::WEEKS);
        words.insert("year", Token::YEAR);
        words.insert("years", Token::YEARS);
        words
    };

    pub static ref CALENDAR_UNIT_ALIAS: HashMap<&'static str, &'static str> = {
        let mut units = HashMap::new();
        units.insert("millisecond", "millisecond");
        units.insert("milliseconds", "millisecond");
        units.insert("second", "second");
        units.insert("seconds", "second");
        units.insert("minute", "minute");
        units.insert("minutes", "minute");
        units.insert("hour", "hour");
        units.insert("hours", "hour");
        units.insert("day", "day");
        units.insert("days", "day");
        units.insert("week", "week");
        units.insert("weeks", "week");
        units.insert("months", "month");
        units.insert("month", "month");
        units.insert("years", "year");
        units.insert("year", "year");

        // the below mappings are for UCUM codes for the same calenda units
        // the below are not used by the scanner but by the SystemQuantity::new() and
        // SystemQuantity::equiv() methods
        units.insert("a", "year");
        units.insert("mo", "month");
        units.insert("wk", "week");
        units.insert("d", "day");
        units.insert("h", "hour");
        units.insert("min", "minute");
        units.insert("s", "second");
        units.insert("ms", "millisecond");
        units
    };
    static ref TIME_RE: Regex = Regex::new(r"^(\d{2}(:\d{2}(:\d{2}(\.\d{3})?)?)?)$").unwrap();
    static ref DATE_RE: Regex = Regex::new(r"^(\d{4}(-\d{2}(-\d{2})?)?)$").unwrap();
    static ref TZ_RE: Regex = Regex::new(r"^(z|Z|(\+|-)\d{2}:\d{2})$").unwrap();
}

#[derive(Debug)]
struct Scanner<'a> {
    filter: Peekable<CharIndices<'a>>,
    errors: Vec<String>
}

#[derive(Debug, PartialEq, Clone)]
#[allow(non_camel_case_types)]
pub enum Token {
    LEFT_PAREN, RIGHT_PAREN,
    LEFT_BRACKET, RIGHT_BRACKET,
    LEFT_BRACE, RIGHT_BRACE,
    COMMA, DOT,
    MINUS, PLUS, AMPERSAND,
    SLASH, STAR,
    NOT, NOT_EQUAL,
    EQUAL,
    EQUIVALENT, NOT_EQUIVALENT,
    GREATER, GREATER_EQUAL,
    LESS, LESS_EQUAL,
    UNION,

    IDENTIFIER(String), QUANTITY(f64, String),
    STRING(String), NUMBER(String), DATE_TIME(SystemDateTime), TIME(SystemTime),

    DIV, MOD, TRUE, FALSE,
    IS, AS, IN, CONTAINS, AND, OR, XOR, IMPLIES,
    CONSTANT(String),
    DOLLAR_THIS, DOLLAR_INDEX, DOLLAR_TOTAL,

    // calendar unit keywords
    DAY, DAYS, HOUR, HOURS, MILLISECOND, MILLISECONDS, MINUTE, MINUTES,
    MONTH, MONTHS, SECOND, SECONDS, WEEK, WEEKS, YEAR, YEARS,

    EOF
}

pub type TokenAndPos = (Token, usize);

#[derive(Debug, Error)]
struct DateTimeParseError {
    msg: String
}

impl DateTimeParseError {
    fn new<S: AsRef<str>>(msg: S) -> Self {
        Self{msg: String::from(msg.as_ref())}
    }
}

impl Display for DateTimeParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

#[derive(Debug, Error)]
struct CalendarUnitParseError {
    msg: String
}

impl CalendarUnitParseError {
    fn new<S: AsRef<str>>(msg: S) -> Self {
        Self{msg: String::from(msg.as_ref())}
    }
}

impl Display for CalendarUnitParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

pub fn scan_tokens(filter: &str) -> Result<VecDeque<TokenAndPos>, ScanError> {
    let chars = filter.char_indices().peekable();
    let mut scanner = Scanner {
        filter: chars,
        errors: vec!(),
    };

    let mut tokens = scanner.scan();
    if !scanner.errors.is_empty() {
        return Err(ScanError{errors: scanner.errors});
    }
    tokens.push_back((EOF, filter.len()));

    Ok(tokens)
}

impl Display for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let TIME(t) = self {
            return f.write_str(t.format(TIME_FORMAT).as_str());
        }
        else if let DATE_TIME(d) = self {
            return f.write_str(d.format(DATETIME_FORMAT).as_str());
        }

        let s=
        match self {
            IDENTIFIER(id) => id.as_str(),
            RIGHT_BRACKET => "]",
            LEFT_BRACKET => "[",
            RIGHT_PAREN => ")",
            LEFT_PAREN => "(",
            EOF => "EOF",
            LEFT_BRACE => "{",
            RIGHT_BRACE => "}",
            COMMA => ",",
            DOT => ".",
            MINUS => "-",
            PLUS => "+",
            AMPERSAND => "&",
            SLASH => "/",
            STAR => "*",
            NOT => "!",
            NOT_EQUAL => "!=",
            EQUAL => "=",
            EQUIVALENT => "~",
            NOT_EQUIVALENT => "!~",
            GREATER => ">",
            GREATER_EQUAL => ">=",
            LESS => "<",
            LESS_EQUAL => "<=",
            UNION => "|",
            STRING(v) => v.as_str(),
            NUMBER(n) => n.as_str(),
            DIV => "div",
            MOD => "mod",
            TRUE => "true",
            FALSE => "false",
            IS => "is",
            AS => "as",
            IN => "in",
            CONTAINS => "contains",
            AND => "and",
            OR => "or",
            XOR => "xor",
            IMPLIES => "implies",
            CONSTANT(c) => c.as_str(),
            DOLLAR_THIS => "$this",
            DOLLAR_INDEX => "$index",
            DOLLAR_TOTAL => "$total",
            DAY => "day",
            DAYS => "days",
            HOUR => "hour",
            HOURS => "hours",
            MILLISECOND => "millisecond",
            MILLISECONDS => "milliseconds",
            MINUTE => "minute",
            MINUTES => "minutes",
            MONTH => "month",
            MONTHS => "months",
            SECOND => "second",
            SECONDS => "seconds",
            WEEK => "week",
            WEEKS => "weeks",
            YEAR => "year",
            YEARS => "years",
            _ => ""
        };

        f.write_str(s)
    }
}

impl Token {
    pub fn lbp(&self) -> usize {
        match *self {
            IMPLIES => 1,
            XOR | OR => 2,
            AND => 3,
            IN => 5,
            CONTAINS => 5,
            EQUAL => 9,
            EQUIVALENT => 9,
            NOT_EQUAL => 9,
            NOT_EQUIVALENT => 9,
            GREATER | GREATER_EQUAL | LESS | LESS_EQUAL => 20,
            UNION => 21,
            IS | AS => 40,
            PLUS | MINUS | AMPERSAND => 45,
            STAR | SLASH | DIV | MOD => 50,
            LEFT_BRACE => 52,
            LEFT_BRACKET => 55,
            DOT => 60,
            LEFT_PAREN  => 75,

            _ => 0
        }
    }
}

impl Scanner<'_> {
    fn scan(&mut self) -> VecDeque<TokenAndPos> {
        let mut tokens: VecDeque<TokenAndPos> = VecDeque::new();
        loop {
            match self.filter.next() {
            Some((pos, c)) => {
                match c {
                    '(' => tokens.push_back((LEFT_PAREN, pos)),
                    ')' => tokens.push_back((RIGHT_PAREN, pos)),
                    '[' => tokens.push_back((LEFT_BRACKET, pos)),
                    ']' => tokens.push_back((RIGHT_BRACKET, pos)),
                    '{' => tokens.push_back((LEFT_BRACE, pos)),
                    '}' => tokens.push_back((RIGHT_BRACE, pos)),
                    '.' => tokens.push_back((DOT, pos)),
                    ',' => tokens.push_back((COMMA, pos)),
                    '-' => tokens.push_back((MINUS, pos)),
                    '+' => tokens.push_back((PLUS, pos)),
                    '*' => tokens.push_back((STAR, pos)),
                    '/' => {
                        if self.match_char('/') { // strip the comment
                            while !self.match_char('\n') {
                                if let None = self.advance() {
                                    break;
                                }
                            }
                        }
                        else if self.match_char('*') { // strip the multi-line comment
                            self.read_multiline_comment(pos);
                        }
                        else {
                            tokens.push_back((SLASH, pos));
                        }
                    },
                    '&' => tokens.push_back((AMPERSAND, pos)),
                    '|' => tokens.push_back((UNION, pos)),
                    '~' => tokens.push_back((EQUIVALENT, pos)),
                    '!' => {
                        if self.match_char('=') {
                            tokens.push_back((NOT_EQUAL, pos));
                        }
                        else if self.match_char('~') {
                            tokens.push_back((NOT_EQUIVALENT, pos));
                        }
                        else {
                            tokens.push_back((NOT, pos));
                        }
                    },
                    '=' => tokens.push_back((EQUAL, pos)),
                    '<' => {
                        if self.match_char('=') {
                            tokens.push_back((LESS_EQUAL, pos));
                        }
                        else {
                            tokens.push_back((LESS, pos));
                        }
                    },
                    '>' => {
                        if self.match_char('=') {
                            tokens.push_back((GREATER_EQUAL, pos));
                        }
                        else {
                            tokens.push_back((GREATER, pos));
                        }
                    },
                    '@' => {
                        if self.match_char('T') {
                            let time = self.scan_time(pos);
                            if let Err(e) = time {
                                self.errors.push(format!("{} at pos {}", e.msg, pos));
                            }
                            else {
                                tokens.push_back((time.unwrap(), pos));
                            }
                        }
                        else {
                            let dt = self.scan_datetime();
                            if let Err(e) = dt {
                                self.errors.push(format!("{} at pos {}", e.msg, pos));
                            }
                            else {
                                tokens.push_back((dt.unwrap(), pos));
                            }
                        }
                    },
                    '\'' => {
                        let t = self.read_string(pos);
                        if let Some(s) = t {
                            let prev = tokens.pop_back();
                            if let Some(prev) = prev {
                                if let NUMBER(n) = prev.0 {
                                    let float = n.parse::<f64>();
                                    if let Err(e) = float {
                                        self.errors.push(format!("invalid value given for quantity '{}' starting at position {}", n, pos));
                                    }
                                    else {
                                        tokens.push_back((QUANTITY(float.unwrap(), s), pos));
                                    }
                                }
                                else {
                                    tokens.push_back(prev);
                                    tokens.push_back((STRING(s), pos));
                                }
                            }
                            else {
                                tokens.push_back((STRING(s), pos));
                            }
                        }
                    },
                    '%' => {
                        let id= self.read_env_var(pos);
                        tokens.push_back((CONSTANT(id), pos));
                    },
                    '`' => {
                        let id = self.read_identifier(' ', pos);
                        if !self.match_char('`') {
                            self.errors.push(format!("missing end quote for identifier '{}' starting at position {}", &id, pos));
                        }
                        else {
                            tokens.push_back((IDENTIFIER(id), pos));
                        }
                    },
                    ' ' | '\t' | '\n' | '\r' => {
                        // eat it
                    },
                    _ => {
                        if self.is_digit(c) {
                            let t = self.read_number(c);
                            tokens.push_back((t, pos));
                        }
                        else {
                            let id = self.read_identifier(c, pos);
                            let id_lowercase = id.to_lowercase();
                            let id_lowercase = id_lowercase.as_str();
                            if let Some(k) = KEYWORDS.get(id_lowercase) {
                                let mut skip = false;
                                // IS and AS can also be used as function calls
                                if id_lowercase == "is" || id_lowercase == "as" || id_lowercase == "contains" {
                                    let prev = tokens.back();
                                    let mut prev_dot = false;
                                    if let Some((prev_token, _)) = prev {
                                        prev_dot = prev_token == &DOT;
                                    }

                                    if prev_dot { // if this is '.' then it is treated as an identifier instead of a keyword
                                        tokens.push_back((IDENTIFIER(id.clone()), pos));
                                        skip = true;
                                    }
                                }

                                if !skip {
                                    self.convert_keyword(&mut tokens, &id, k, pos);
                                }
                            }
                            else {
                                tokens.push_back((IDENTIFIER(id), pos));
                            }
                        }
                    }
                }
            },
              None => {
                    break;
              }
            }
        }
        tokens
    }

    fn read_number(&mut self, first_digit: char) -> Token {
        let mut n = String::new();
        n.push(first_digit);
        loop {
            match self.filter.peek() {
                Some((pos, c)) => {
                    let c = *c;
                    if self.is_digit(c) || c == '.' {
                        n.push(c);
                        self.advance();
                    }
                    else {
                        break;
                    }
                },
                None => {
                    break;
                }
            }
        }

        NUMBER(n)
    }

    fn read_identifier(&mut self, first_char: char, start: usize) -> String {
        let mut id = String::new();
        if first_char != ' ' {
            id.push(first_char);
        }

        loop {
            match self.filter.peek() {
                Some((_, c)) => {
                    let c = *c;
                    if self.is_alpha_numeric(c) {
                        self.filter.next();
                        id.push(c);
                    }
                    else {
                        break;
                    }
                },
                None => {
                    break;
                }
            }
        }

        if id.len() == 0 {
            self.errors.push(format!("invalid identifier '{}' starting at position {}", &id, start));
        }

        id
    }

    fn read_string(&mut self, start: usize) -> Option<String> {
        let mut prev: char = '\'';
        let mut s = String::new();
        loop {
            match self.filter.next() {
                Some((pos, c)) => {
                    if c == '\'' && prev != '\\' {
                        break;
                    }
                    if prev == '\\' {
                        s.pop(); // remove the \
                        match c {
                            'r' => s.push('\r'),
                            'n' => s.push('\n'),
                            't' => s.push('\t'),
                            'f' => s.push_str("\\f"),
                            'u' | 'U' => {
                                s.push(c);
                            },
                            _ => {
                                s.push(c);
                            }
                        }
                    }
                    else {
                        s.push(c);
                    }
                    prev = c;
                },
                None => {
                    self.errors.push(format!("invalid string '{}' starting at position {}", &s, start));
                    return Option::None;
                }
            }
        }

        Option::Some(s)
    }

    fn read_multiline_comment(&mut self, start: usize) {
        loop  {
            match self.filter.next() {
                Some((_, a)) => {
                    if self.match_char('*') {
                        if self.match_char('/') {
                            break;
                        }
                    }
                },
                None => {
                    self.errors.push(format!("multiline comment starting at {} was not closed properly", start));
                    break;
                }
            }
        }
    }

    fn read_env_var(&mut self, start: usize) -> String {
        let id;
        if self.match_char('`') {
            id = self.read_identifier(' ', start);
            if !self.match_char('`') {
                self.errors.push(format!("missing end backtick for constant '{}' starting at position {}", &id, start));
            }
        }
        else if self.match_char('\'') {
            id = self.read_identifier(' ', start);
            if !self.match_char('\'') {
                self.errors.push(format!("missing end quote for constant '{}' starting at position {}", &id, start));
            }
        }
        else {
            id = self.read_identifier(' ', start);
        }

        id
    }

    #[inline]
    fn scan_time(&mut self, pos: usize) -> Result<Token, DateTimeParseError> {
        let mut tmp = String::with_capacity(12);
        loop {
            match self.filter.next() {
                Some((pos, c)) => {
                    if c == ' ' {
                        break;
                    }
                    tmp.push(c);
                },
                None => break
            }
        }

        let mut precision: u8 = 0;
        let (nt, precision) = self.parse_time(tmp.as_str(), precision)?;
        Ok(TIME(SystemTime::new(nt, precision)))
    }

    #[inline]
    fn parse_time(&self, time_str: &str, mut precision: u8) -> Result<(NaiveTime, u8), DateTimeParseError> {
        if !TIME_RE.is_match(time_str) {
            return Err(DateTimeParseError::new("invalid time"));
        }

        let mut hour: Option<u32> = None;
        let mut min: Option<u32> = None;
        let mut sec: Option<u32> = None;
        let mut milli: Option<u32> = None;

        for p in  time_str.split(|c| c == ':' || c == '.') {
            if hour.is_none() {
                hour = Some(self.parse_u32(p, 24,"hours")?);
                precision |= 4;
            }
            else if min.is_none() {
                min = Some(self.parse_u32(p, 60,"minutes")?);
                precision |= 2;
            }
            else if sec.is_none() {
                sec = Some(self.parse_u32(p, 60,"seconds")?);
                precision |= 1;
            }
            else if milli.is_none() {
                milli = Some(self.parse_u32(p, 999, "milliseconds")?);
            }
        }

        let hour = hour.unwrap_or(0);
        let min = min.unwrap_or(0);
        let sec = sec.unwrap_or(0);
        let milli = milli.unwrap_or(0);
        let nt = NaiveTime::from_hms_milli(hour, min, sec, milli);
        Ok((nt, precision))
    }

    /// parses the format YYYY-MM-DDThh:mm:ss.fff(+|-)hh:mm
    #[inline]
    fn scan_datetime(&mut self) -> Result<Token, DateTimeParseError> {
        let mut tmp = String::with_capacity(33);
        loop {
            match self.filter.next() {
                Some((pos, c)) => {
                    if c == ' ' {
                        break;
                    }
                    tmp.push(c);
                },
                None => break
            }
        }

        let mut parts = tmp.splitn(2, "T");
        let date_part = parts.next();
        if let None = date_part {
            return Err(DateTimeParseError::new("invalid datetime, missing date"));
        }
        let date_part = date_part.unwrap();
        if !DATE_RE.is_match(date_part) {
            return Err(DateTimeParseError::new("invalid datetime format"));
        }

        let mut precision: u8 = 0;
        let mut parsed = Parsed::default();
        for p in date_part.split("-") {
            if parsed.year.is_none() {
                let y = self.parse_u32(p, 9999, "year")?;
                parsed.set_year(y as i64);
                precision |= 32;
            }
            else if parsed.month.is_none() {
                let m = self.parse_u32(p, 12, "month")?;
                parsed.set_month(m as i64);
                precision |= 16;
            }
            else if parsed.day.is_none() {
                let d = self.parse_u32(p, 31, "day")?;
                parsed.set_day(d as i64);
                precision |= 8;
            }
        }

        let mut offset = 0;
        let mut nt = NaiveTime::from_hms_milli(0, 0, 0, 0);
        let time_with_tz_part = parts.next();
        if let Some(time_with_tz_part) = time_with_tz_part {
            if time_with_tz_part != "" {
                let mut time_parts = time_with_tz_part.splitn(2, |c| c == 'z' || c == 'Z' || c ==  '+' || c ==  '-');
                let time_part = time_parts.next().unwrap();
                let tmp = self.parse_time(time_part, precision)?;
                nt = tmp.0;
                precision = tmp.1;

                if let Some(_) = time_parts.next() {
                    let tz_part = &time_with_tz_part[time_part.len()..]; // this is required to preserve the z, Z, + or - char in the front
                    offset = self.parse_tz(tz_part)?;
                    let pr = parsed.set_offset(offset);
                    if let Err(e) = pr {
                        return Err(DateTimeParseError::new(format!("invalid datetime, {}", e.to_string())));
                    }
                }
            }
        }

        if let None = parsed.offset {
            parsed.set_offset(0);
        }

        parsed.set_hour(nt.hour() as i64);
        parsed.set_minute(nt.minute() as i64);
        parsed.set_second(nt.second() as i64);
        parsed.set_nanosecond(nt.nanosecond() as i64);

        if parsed.month.is_none() {
            parsed.set_month(1);
        }

        if parsed.day.is_none() {
            parsed.set_day(1);
        }
        let dt = parsed.to_datetime();
        if let Err(e) = dt {
            return Err(DateTimeParseError::new(e.to_string()));
        }

        let dt = dt.unwrap().with_timezone(&Utc);
        Ok(DATE_TIME(SystemDateTime::new(dt, precision)))
    }

    #[inline]
    fn parse_tz(&self, tz_val: &str) -> Result<i64, DateTimeParseError> {
        if !TZ_RE.is_match(tz_val) {
            return Err(DateTimeParseError::new("invalid timezone offset in datetime"));
        }

        let mut offset: i64 = 0;
        if tz_val != "z" && tz_val != "Z" {
            let mut tz_parts = tz_val[1..].split(":");
            let hours = tz_parts.next().unwrap();
            let hours = self.parse_u32(hours, 12, "tz hours")?;
            let min = tz_parts.next().unwrap();
            let min = self.parse_u32(min, 59, "tz minutes")?;

            offset = (hours * 3600 + min * 60) as i64;
            if tz_val.starts_with('-') {
                offset = -offset;
            }
        }

        Ok(offset)
    }

    #[inline]
    fn convert_keyword(&mut self, tokens: &mut VecDeque<TokenAndPos>, name: &String, current: &Token, pos: usize) {
        match current {
            SECOND | SECONDS | MILLISECOND | MILLISECONDS | MINUTE | MINUTES |
            HOUR | HOURS | DAY | DAYS | MONTH | MONTHS | YEAR | YEARS => {
                let prev = tokens.pop_back();
                if let None = prev {
                    self.errors.push(format!("invalid placement of calendar unit {}", name));
                }
                else {
                    let prev = prev.unwrap();
                    if let NUMBER(n) = prev.0 {
                        let float = n.parse::<f64>();
                        if let Err(e) = float {
                            self.errors.push(format!("invalid value given for calendar quantity '{}' starting at position {}", n, pos));
                        }
                        else {
                            let alias = CALENDAR_UNIT_ALIAS.get(name.as_str());
                            if let None = alias {
                                self.errors.push(format!("invalid calendar unit {}", name));
                            }
                            else {
                                let name = String::from(*alias.unwrap());
                                tokens.push_back((QUANTITY(float.unwrap(), name), pos));
                            }
                        }
                    }
                    else {
                        self.errors.push(format!("invalid token present before calendar quantity '{}' starting at position {}", prev.0, pos));
                    }
                }
            },
            _ => {
                tokens.push_back((current.clone(), pos));
            }
        }
    }

    #[inline]
    fn advance(&mut self) -> Option<(usize, char)> {
        self.filter.next()
    }

    #[inline]
    fn match_char(&mut self, expected: char) -> bool {
        let c = self.filter.peek();
        if c.is_none() {
            return false;
        }
        let (_, c) = *c.unwrap();
        if c == expected {
            self.advance();
            return true;
        }

        false
    }

    #[inline]
    fn is_alpha_numeric(&self, c: char) -> bool {
        self.is_alpha(c) || self.is_digit(c)
    }

    #[inline]
    fn is_alpha(&self, c: char) -> bool {
        (c >= 'a' && c <= 'z') ||
        (c >= 'A' && c <= 'Z') ||
        c == '_'
    }

    #[inline]
    fn is_digit(&self, c: char) -> bool {
        return c >= '0' && c <= '9';
    }

    #[inline]
    fn parse_u32(&self, val: &str, max: u32, field_name: &str) -> Result<u32, DateTimeParseError> {
        let num = val.parse::<u32>();
        if let Err(e) = num {
            return Err(DateTimeParseError{msg: format!("could not parse value {} of {} {}", val, field_name, e.to_string())});
        }

        let num = num.unwrap();
        if num > max {
            return Err(DateTimeParseError::new(format!("invalid value {} given for {}", val, field_name)));
        }

        Ok(num)
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use anyhow::Error;
    use chrono::{DateTime, NaiveTime, Utc};

    use super::*;

    struct FilterCandidate<'a> {
        filter: &'a str,
        token_count: usize,
        error_count: usize
    }
    #[test]
    fn test_scaning() {
        let mut candidates: Vec<FilterCandidate> = vec!();
        let c1 = FilterCandidate{ filter: "1+1", token_count: 4, error_count: 0};
        candidates.push(c1);
        let c1 = FilterCandidate{ filter: "Patient.name.first(1+1)", token_count: 11, error_count: 0};
        candidates.push(c1);

        println!("begin scanning");
        for c in &candidates {
            let r = scan_tokens(c.filter);
            if r.is_ok() {
                let tokens = r.as_ref().unwrap();
                println!("{:?}", &tokens);
            }
            else {
                let se = r.as_ref().err().unwrap();
                println!("{:?}", &se);
            }
            if c.error_count != 0 {
                let se = r.err().unwrap();
                assert_eq!(c.error_count, se.errors.len());
            }
            else {
                assert!(r.is_ok());
                let tokens = r.unwrap();
                assert_eq!(c.token_count, tokens.len());
            }
        }
    }

    #[test]
    fn test_using_abnfgen() {
        let mut abnfgen = Command::new("abnfgen");
        abnfgen.arg("-c").arg("-y 20").arg("fhirpath.abnf");
        if abnfgen.output().is_err() {
            println!("abnfgen command failed, skipping fuzzing of filter scanner. Check the path of abnfgen and try again.");
            return;
        }

        let n = 200;
        println!("testing scanner with {} generated filters", n);
        for _ in 1..n {
            let out = abnfgen.output().unwrap();
            let filter = String::from_utf8(out.stdout).unwrap();
            let filter = filter.replace("\n", "");
            let filter = filter.replace("\r", "");
            println!("scanning: {}", filter);
            let r = scan_tokens(filter.as_str());
            if r.is_err() {
                let se = r.err().unwrap();
                println!("{:?}\n{}", &se, filter);
            }
            assert!(true);
        }
    }

    #[test]
    fn test_string_escape() -> Result<(), Error> {
        let mut candidates: Vec<(&str, &str)> = Vec::new();
        candidates.push(("'it\\'s me'", "it's me"));
        candidates.push(("'it\\\"s me'", "it\"s me"));
        candidates.push(("'it\\`s me'", "it`s me"));
        candidates.push(("'it\\rs me'", "it\rs me"));
        candidates.push(("'look \\U+092E, this is unicode'", "look U+092E, this is unicode"));
        candidates.push(("'this char \\C is an non-escape char'", "this char C is an non-escape char"));
        candidates.push(("'linefeed char \\f is treated differently'", "linefeed char \\f is treated differently"));
        for (input, expected) in candidates {
            let r = scan_tokens(input)?.pop_front().unwrap();
            if let Token::STRING(actual) = r.0 {
                assert_eq!(expected, actual);
            }
            else {
                assert!(false, format!("unexpected token received {}", r.0));
            }
        }

        let err = scan_tokens("'this is an invalid string");
        assert!(err.is_err());

        Ok(())
    }

    #[test]
    fn test_parse_time() -> Result<(), Error> {
        let mut time_candidates: Vec<(&str, bool)> = Vec::new();
        time_candidates.push(("14:58:00.100", true));
        time_candidates.push(("14", true));
        time_candidates.push(("1", false));
        time_candidates.push(("11:", false));
        for (input, expected) in time_candidates {
            let actual = TIME_RE.is_match(input);
            assert_eq!(expected, actual);
        }

        let mut candidates: Vec<(&str, NaiveTime)> = Vec::new();
        candidates.push(("@T14:58:00.100", NaiveTime::from_hms_milli(14, 58, 00, 100)));
        candidates.push(("@T14:58:00.200 ", NaiveTime::from_hms_milli(14, 58, 00, 200)));
        candidates.push(("@T01", NaiveTime::from_hms_milli(1, 0, 0, 0)));
        for (input, expected) in candidates {
            let r = scan_tokens(input)?.pop_front().unwrap();
            if let Token::TIME(actual) = r.0 {
                assert_eq!(expected.format(TIME_FORMAT).to_string(), actual.format(TIME_FORMAT));
            }
            else {
                assert!(false, format!("unexpected token received {}", r.0));
            }
        }

        let mut err_candidates = Vec::new();
        err_candidates.push("@T");
        err_candidates.push("@T1");
        err_candidates.push("@T1:1:0");
        err_candidates.push("@T1.1:0.");
        err_candidates.push("@T1:1:0.");
        err_candidates.push("@T1:1:");

        for input in err_candidates {
            let r = scan_tokens(input);
            assert!(r.is_err());
        }

        Ok(())
    }

    #[test]
    fn test_parse_datetime() -> Result<(), Error> {
        let mut date_candidates: Vec<(&str, bool)> = Vec::new();
        date_candidates.push(("2022-02-10", true));
        date_candidates.push(("2022-02", true));
        date_candidates.push(("2022", true));
        date_candidates.push(("2022-", false));
        date_candidates.push(("2022-02-", false));
        date_candidates.push(("2022-02-10-", false));
        for (input, expected) in date_candidates {
            let actual = DATE_RE.is_match(input);
            assert_eq!(expected, actual);
        }

        let mut candidates: Vec<(&str, &DateTime<Utc>)> = Vec::new();
        let dt = &Utc.ymd(2022, 2, 10).and_hms_milli(14, 58, 0, 100);
        candidates.push(("@2022-02-10T14:58:00.100", dt));
        candidates.push(("@2022-02-10T14:58:00.100Z", dt));
        candidates.push(("@2022-02-10T14:58:00.100z", dt));
        candidates.push(("@2022-02-10T14:58:00.100+00:00", dt));
        candidates.push(("@2022-02-10T14:58:00.100-00:00", dt));
        candidates.push(("@2022-02-10T15:58:00.100+01:00", dt)); // GMT + 1
        candidates.push(("@2022-02-10T10:58:00.100-04:00", dt)); // GMT - 4
        candidates.push(("@2022-02-10T10:28:00.100-04:30", dt)); // GMT - 4:30

        let date_without_dm = &Utc.ymd(2022, 1, 1).and_hms_milli(14, 58, 0, 100);
        candidates.push(("@2022T14:58:00.100", date_without_dm));

        let date_without_time = &Utc.ymd(2022, 2, 10).and_hms_milli(0, 0, 0, 0);
        candidates.push(("@2022-02-10T", date_without_time));
        let date_without_time_and_day = &Utc.ymd(2022, 2, 1).and_hms_milli(0, 0, 0, 0);
        candidates.push(("@2022-02T", date_without_time_and_day));

        let date_without_tdm = &Utc.ymd(2022, 1, 1).and_hms_milli(0, 0, 0, 0);
        candidates.push(("@2022T", date_without_tdm));

        for (input, expected) in candidates {
            let r = scan_tokens(input).unwrap().pop_front().unwrap();
            if let Token::DATE_TIME(ref actual) = r.0 {
                assert_eq!(expected.format(DATETIME_FORMAT).to_string(), actual.format(DATETIME_FORMAT));
            }
            else {
                assert!(false, format!("unexpected token received {}", r.0));
            }
        }

        let mut err_candidates = Vec::new();
        err_candidates.push("@abcdT12:11:00.000Z");
        err_candidates.push("@2022-T12:11:00.000Z");

        for input in err_candidates {
            let r = scan_tokens(input);
            assert!(r.is_err());
        }

        Ok(())
    }

    #[test]
    fn test_parse_quantity() -> Result<(), Error> {
        let mut candidates: Vec<(&str, f64, &str)> = Vec::new();
        candidates.push(("4.5 'mg'", 4.5, "mg"));
        candidates.push(("1 'year'", 1.0, "year"));
        candidates.push(("1.001 'g'", 1.001, "g"));
        for (input, expected_val, expected_code) in candidates {
            let r = scan_tokens(input).unwrap().pop_front().unwrap();
            if let QUANTITY(actual_val, actual_code) = r.0 {
                assert_eq!(expected_val, actual_val);
                assert_eq!(expected_code, actual_code.as_str());
            }
            else {
                assert!(false, format!("unexpected token received {}", r.0));
            }
        }

        // let mut err_candidates = Vec::new();
        // err_candidates.push("1.a 'g'");
        // for input in err_candidates {
        //     let r = scan_tokens(input);
        //     assert!(r.is_err());
        // }

        Ok(())
    }

    #[test]
    fn test_system_datetime_equality() -> Result<(), Error> {
        let mut candidates: Vec<(&str, u8, &DateTime<Utc>)> = Vec::new();
        let dt = &Utc.ymd(2022, 2, 10).and_hms_milli(14, 58, 0, 100);
        candidates.push(("@2022-02-10T14:58:00.100", 63, dt));
        candidates.push(("@2022-02-10T14:58:00.100Z", 63, dt));

        for (input, precision, expected) in candidates {
            let r = scan_tokens(input)?.pop_front().unwrap();
            if let Token::DATE_TIME(ref actual) = r.0 {
                let expected = &SystemDateTime::new(*expected, precision);
                let r = SystemDateTime::equals(expected, actual);
                assert!(r.as_bool().unwrap());
            }
            else {
                assert!(false, format!("unexpected token received {}", r.0));
            }
        }

        // missing seconds and milliseconds part should result in empty result
        let r = scan_tokens("@2022-02-10T14:58")?.pop_front().unwrap();
        if let Token::DATE_TIME(ref actual) = r.0 {
            let expected = &SystemDateTime::new(*dt, 63);
            let r = SystemDateTime::equals(expected, actual);
            assert!(r.is_empty());
        }

        let mut candidates: Vec<(&str, u8, NaiveTime)> = Vec::new();
        candidates.push(("@T14:58:00.100", 7, NaiveTime::from_hms_milli(14, 58, 00, 100)));
        candidates.push(("@T14:58:00.200 ", 7, NaiveTime::from_hms_milli(14, 58, 00, 200)));
        candidates.push(("@T01", 4, NaiveTime::from_hms_milli(1, 0, 0, 0)));
        for (input, precision, expected) in candidates {
            let r = scan_tokens(input)?.pop_front().unwrap();
            if let Token::TIME(ref actual) = r.0 {
                let expected = &SystemTime::new(expected, precision);
                let r = SystemTime::equals(expected, actual);
                assert!(r.as_bool().unwrap());
            }
            else {
                assert!(false, format!("unexpected token received {}", r.0));
            }
        }

        Ok(())
    }
}