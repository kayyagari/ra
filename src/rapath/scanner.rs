use std::collections::HashMap;
use lazy_static::lazy_static;
use std::fmt::{Display, Formatter};
use std::error::Error;

lazy_static! {
 pub static ref KEYWORDS: HashMap<&'static str, TokenType> = {
        let mut words = HashMap::new();
        words.insert("div", TokenType::DIV);
        words.insert("mod", TokenType::MOD);
        words.insert("is", TokenType::IS);
        words.insert("as", TokenType::AS);
        words.insert("in", TokenType::IN);
        words.insert("contains", TokenType::CONTAINS);
        words.insert("and", TokenType::AND);
        words.insert("or", TokenType::OR);
        words.insert("xor", TokenType::XOR);
        words.insert("implies", TokenType::IMPLIES);
        words.insert("true", TokenType::TRUE);
        words.insert("false", TokenType::FALSE);
        words.insert("$this", TokenType::DOLLAR_THIS);
        words.insert("$index", TokenType::DOLLAR_INDEX);
        words.insert("$total", TokenType::DOLLAR_TOTAL);

        words.insert("day", TokenType::DAY);
        words.insert("days", TokenType::DAYS);
        words.insert("hour", TokenType::HOUR);
        words.insert("hours", TokenType::HOURS);
        words.insert("millisecond", TokenType::MILLISECOND);
        words.insert("milliseconds", TokenType::MILLISECONDS);
        words.insert("minute", TokenType::MINUTE);
        words.insert("minutes", TokenType::MINUTES);
        words.insert("month", TokenType::MONTH);
        words.insert("months", TokenType::MONTHS);
        words.insert("second", TokenType::SECOND);
        words.insert("seconds", TokenType::SECONDS);
        words.insert("week", TokenType::WEEK);
        words.insert("weeks", TokenType::WEEKS);
        words.insert("year", TokenType::YEAR);
        words.insert("years", TokenType::YEARS);
        words
    };
}

#[derive(Debug)]
struct Scanner {
    start: usize,
    current: usize,
    len: usize,
    filter: Vec<char>,
    errors: Vec<String>
}


#[derive(Debug)]
pub struct ScanError {
    pub errors: Vec<String>
}

#[derive(Debug)]
pub struct Token {
    pub val: String,
    pub ttype: TokenType,
}

#[derive(Debug, PartialEq, Copy, Clone)]
#[allow(non_camel_case_types)]
pub enum TokenType {
    LEFT_PAREN, RIGHT_PAREN,
    LEFT_BRACKET, RIGHT_BRACKET,
    LEFT_BRACE, RIGHT_BRACE,
    COMMA, DOT,
    MINUS, PLUS, AMPERSAND,
    SLASH, STAR,
    NOT, NOT_EQUAL,
    EQUAL, EQUAL_EQUAL,
    EQUIVALENT, NOT_EQUIVALENT,
    GREATER, GREATER_EQUAL,
    LESS, LESS_EQUAL,

    IDENTIFIER,
    STRING, NUMBER, DATE, TIME,

    DIV, MOD, TRUE, FALSE,
    IS, AS, IN, CONTAINS, AND, OR, XOR, IMPLIES,
    CONSTANT,
    DOLLAR_THIS, DOLLAR_INDEX, DOLLAR_TOTAL,

    // calendar unit keywords
    DAY, DAYS, HOUR, HOURS, MILLISECOND, MILLISECONDS, MINUTE, MINUTES,
    MONTH, MONTHS, SECOND, SECONDS, WEEK, WEEKS, YEAR, YEARS,

    EOF
}

pub fn scan_tokens(filter: &String) -> Result<Vec<Token>, ScanError> {
    // this copying is unavoidable because no other format gives the
    // ability to index into the input string
    let chars: Vec<char> = filter.chars().collect();
    let mut scanner = Scanner {
        start: 0,
        current: 0,
        len: chars.len(),
        filter: chars,
        errors: vec!(),
    };

    let mut tokens: Vec<Token> = vec!();
    scanner.scan(&mut tokens);
    if !scanner.errors.is_empty() {
        return Err(ScanError{errors: scanner.errors});
    }

    Ok(tokens)
}

impl Error for ScanError{
    fn description(&self) -> &str {
        "filter parsing error"
    }
}

impl Display for ScanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for e in &self.errors {
            f.write_str(e.as_str())?;
        }
        Ok(())
    }
}

impl Display for TokenType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s;
        match self {
            TokenType::IDENTIFIER => {
                s = "identifier";
            },
            TokenType::RIGHT_BRACKET => {
                s = "]";
            },
            TokenType::LEFT_BRACKET => {
                s = "[";
            },
            TokenType::RIGHT_PAREN => {
                s = ")";
            },
            TokenType::LEFT_PAREN => {
                s = "(";
            },
            TokenType::EOF => {
                s = "EOF";
            },
            _ => {
              s = "unsupported token";
            }
        }

        f.write_str(s)
    }
}

impl Scanner {
    fn scan(&mut self, tokens: &mut Vec<Token>) {
        while !self.is_at_end() {
            self.start = self.current;
            let t = self.scan_token();
            if t.is_some() {
                tokens.push(t.unwrap());
            }
        }
    }

    fn scan_token(&mut self) -> Option<Token> {
        let c = self.advance();
        let mut t: Option<Token> = Option::None;
        match c {
            '(' => {
                t = Option::Some(Token { val: String::from('('), ttype: TokenType::LEFT_PAREN });
            }
            ')' => {
                t = Option::Some(Token { val: String::from(')'), ttype: TokenType::RIGHT_PAREN });
            }
            '[' => {
                t = Option::Some(Token { val: String::from('['), ttype: TokenType::LEFT_BRACKET });
            }
            ']' => {
                t = Option::Some(Token { val: String::from(']'), ttype: TokenType::RIGHT_BRACKET });
            },
            '{' => {
                t = Option::Some(Token{ val: String::from('{'), ttype: TokenType::LEFT_BRACE});
            },
            '}' => {
                t = Option::Some(Token{ val: String::from('}'), ttype: TokenType::RIGHT_BRACE});
            },
            '.' => {
                t = Option::Some(Token{ val: String::from('.'), ttype: TokenType::DOT});
            },
            ',' => {
                t = Option::Some(Token{ val: String::from(','), ttype: TokenType::COMMA});
             },
            '-' => {
                t = Option::Some(Token{ val: String::from('-'), ttype: TokenType::MINUS});
             },
            '+' => {
                t = Option::Some(Token{ val: String::from('+'), ttype: TokenType::PLUS});
             },
            '*' => {
                t = Option::Some(Token{ val: String::from('*'), ttype: TokenType::STAR});
             },
            '/' => {
                let next = self.peek();
                if next =='/' { // strip the comment
                    self.advance();
                    while self.peek() != '\n' && !self.is_at_end() {
                        self.advance();
                    }
                }
                else if next == '*' { // strip the multi-line comment
                    self.advance();
                    while !self.is_at_end() {
                        let next = self.peek();
                        if next == '*' {
                            let next = self.peek_double();
                            if next == '/' {
                                self.advance();
                                self.advance();
                                break;
                            }
                        }
                        self.advance();
                    }
                }
                else {
                    t = Option::Some(Token{ val: String::from('/'), ttype: TokenType::SLASH});
                }
            },
            '&' => {
                t = Option::Some(Token{ val: String::from('&'), ttype: TokenType::AMPERSAND});
            },
            '|' => {
                t = Option::Some(Token{ val: String::from('|'), ttype: TokenType::OR});
            },
            '~' => {
                t = Option::Some(Token{ val: String::from('~'), ttype: TokenType::EQUIVALENT});
            },
            '!' => {
                let next = self.peek();
                if next == '=' {
                    t = self.create_token ("!=", TokenType::NOT_EQUAL);
                    self.advance();
                }
                else if next == '~' {
                    t = self.create_token ("!~", TokenType::NOT_EQUIVALENT);
                    self.advance();
                }
                else {
                    t = self.create_token ("!", TokenType::NOT);
                }
            },
            '=' => {
                t = self.create_token ("=", TokenType::EQUAL);
            },
            '<' => {
                if self.match_char('=') {
                    t = self.create_token ("<=", TokenType::LESS_EQUAL);
                }
                else {
                    t = self.create_token ("<", TokenType::LESS);
                }
            },
            '>' => {
                if self.match_char('=') {
                    t = self.create_token (">=", TokenType::GREATER_EQUAL);
                }
                else {
                    t = self.create_token (">", TokenType::GREATER);
                }
            },
            '@' => {
                //t = self.read_datetime();
            },
            '\'' => {
                t = self.read_string();
            },
            '%' => {
                t = self.read_identifier();
                t.as_mut().unwrap().ttype = TokenType::CONSTANT;
            },
            ' ' | '\t' | '\n' => {
                // eat it
            },
            _ => {
                if self.is_digit(c) {
                    t = self.read_number();
                }
                else {
                    t = self.read_identifier();
                }
            }
        }

        t
    }

    fn read_number(&mut self) -> Option<Token> {
        let begin = self.start;
        while self.is_digit(self.peek()) {
            self.advance();
        }

        if self.peek() == '.' && self.is_digit(self.peek_double()) {
            self.advance();

            while self.is_digit(self.peek()) {
                self.advance();
            }
        }

        let val: String = self.filter[begin .. self.current].iter().collect();
        let t = Token{val, ttype: TokenType::NUMBER};
        Option::Some(t)
    }

    fn read_identifier(&mut self) -> Option<Token> {
        while self.is_alpha_numeric(self.peek()) {
            self.advance();
        }

        let mut val: String = self.filter[self.start .. self.current].iter().collect();
        let mut tt: TokenType = TokenType::IDENTIFIER;
        if let Some(k) = KEYWORDS.get(val.as_str()) {
            tt = *k;
        }

        Option::Some(Token { val, ttype: tt })
    }

    fn read_string(&mut self) -> Option<Token> {
        let mut prev: char = '\'';
        let mut c: char = '\0';
        let mut val: Vec<char> = vec!();
        while !self.is_at_end() {
            c = self.peek();
            if c == '\'' && prev != '\\' {
                break;
            }
            c = self.advance();
            match c {
                '\\' => {
                    if prev == '\\' {
                        val.push(c);
                    }
                }
                _ => {
                    val.push(c);
                }
            }
            prev = c;
        }

        if self.is_at_end() || c != '\'' {
            let s: String = val.iter().collect();
            self.errors.push(format!("invalid string '{}' starting at position {}", s, self.start));
            return Option::None;
        }

        self.advance();

        Option::Some(Token { val: val.iter().collect(), ttype: TokenType::STRING })
    }

    fn advance(&mut self) -> char {
        let c = self.filter[self.current];
        self.current += 1;
        c
    }

    fn peek(&self) -> char {
        if self.is_at_end() {
            return '\0';
        }

        self.filter[self.current]
    }

    fn peek_double(&self) -> char {
        let i = self.current + 2;
        if i >= self.len {
            return '\0';
        }
        self.filter[i]
    }

    fn match_char(&mut self, expected: char) -> bool {
        if self.is_at_end() {
            return false;
        }

        if self.filter[self.current] != expected {
            return false;
        }

        self.current += 1;
        return true;
    }

    fn is_at_end(&self) -> bool {
        self.current >= self.len
    }

    fn is_alpha_numeric(&self, c: char) -> bool {
        self.is_alpha(c) || self.is_digit(c)
    }

    fn is_alpha(&self, c: char) -> bool {
        (c >= 'a' && c <= 'z') ||
        (c >= 'A' && c <= 'Z') ||
        c == '_'
    }

    fn is_digit(&self, c: char) -> bool {
        return c >= '0' && c <= '9';
    }

    fn create_token(&self, val: &str, ttype: TokenType) -> Option<Token> {
        Option::Some(Token{ val: String::from(val), ttype})
    }
}

#[cfg(test)]
mod tests {
    use crate::scanner::scan_tokens;
    use std::process::Command;

    struct FilterCandidate {
        filter: String,
        token_count: usize,
        error_count: usize
    }
    #[test]
    fn test_scaning() {
        let mut candidates: Vec<FilterCandidate> = vec!();
        let c1 = FilterCandidate{ filter: String::from("1+1"), token_count: 3, error_count: 0};

        println!("begin scanning");
        for c in &candidates {
            let r = scan_tokens(&c.filter);
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

    //#[test]
    fn test_using_abnfgen() {
        let mut abnfgen = Command::new("abnfgen");
        abnfgen.arg("-c").arg("search-filter.abnf");
        if abnfgen.output().is_err() {
            println!("abnfgen command failed, skipping fuzzing of filter scanner. Check the path of abnfgen and try again.");
            return;
        }

        let n = 2000;
        println!("testing scanner with {} generated filters", n);
        for _ in 1..n {
            let out = abnfgen.output().unwrap();
            let filter = String::from_utf8(out.stdout).unwrap();
            let filter = filter.replace("\n", "");
            let filter = filter.replace("\r", "");
            //println!("scanning: {}", &filter);
            let r = scan_tokens(&filter);
            if r.is_err() {
                let se = r.err().unwrap();
                println!("{:?}\n{}", &se, filter);
            }
            assert!(true);
        }
    }
}