use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::iter::Peekable;
use std::str::CharIndices;

use lazy_static::lazy_static;

use crate::errors::ScanError;

use self::Token::*;

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

    IDENTIFIER(String),
    STRING(String), NUMBER(String), DATE(String), TIME(String),

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
            DATE(d) => d.as_str(),
            TIME(t) => t.as_str(),
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
            YEARS => "years"
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
                        //t = self.read_datetime();
                    },
                    '\'' => {
                        let t = self.read_string(pos);
                        if let Some(s) = t {
                            tokens.push_back((s, pos));
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
                            if let Some(k) = KEYWORDS.get(id.to_lowercase().as_str()) {
                                tokens.push_back((k.clone(), pos));
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

    fn read_string(&mut self, start: usize) -> Option<Token> {
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

        Option::Some(STRING(s))
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
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use anyhow::Error;

    use crate::rapath::scanner::{scan_tokens, Token};

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
    fn test_string_escape() -> Result<(), Error>{
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

        Ok(())
    }
}