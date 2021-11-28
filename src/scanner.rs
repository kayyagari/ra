use std::str::Chars;
use std::collections::HashMap;
use lazy_static::lazy_static;
use std::fmt::{Display, Formatter};
use std::error::Error;

lazy_static! {
 static ref operators: HashMap<&'static str, Operator> = {
        let mut ops = HashMap::new();
        ops.insert("eq", Operator::EQ);
        ops.insert("ne", Operator::NE);
        ops.insert("co", Operator::CO);
        ops.insert("sw", Operator::SW);
        ops.insert("ew", Operator::EW);
        ops.insert("gt", Operator::GT);
        ops.insert("lt", Operator::LT);
        ops.insert("ge", Operator::GE);
        ops.insert("le", Operator::LE);
        ops.insert("ap", Operator::AP);
        ops.insert("sa", Operator::SA);
        ops.insert("eb", Operator::EB);
        ops.insert("pr", Operator::PR);
        ops.insert("po", Operator::PO);
        ops.insert("ss", Operator::SS);
        ops.insert("sb", Operator::SB);
        ops.insert("in", Operator::IN);
        ops.insert("ni", Operator::NI);
        ops.insert("re", Operator::RE);
        ops
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
pub enum Operator {
    EQ,
    NE,
    CO,
    SW,
    EW,
    GT,
    LT,
    GE,
    LE,
    AP,
    SA,
    EB,
    PR,
    PO,
    SS,
    SB,
    IN,
    NI,
    RE
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

#[derive(Debug, PartialEq)]
pub enum TokenType {
    LEFT_PAREN,
    RIGHT_PAREN,
    LEFT_BRACKET,
    RIGHT_BRACKET,
    LITERAL,
    IDENTIFIER,
    OPERATOR,
    FILTER,
    IDENTIFIER_PATH,
    EOF
}

pub fn scanTokens(filter: &String) -> Result<Vec<Token>, ScanError> {
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
            f.write_str(e.as_str());
        }
        Ok(())
    }
}

impl Scanner {
    fn scan(&mut self, tokens: &mut Vec<Token>) {
        while !self.is_at_end() {
            self.start = self.current;
            let t = self.scanToken();
            if t.is_some() {
                tokens.push(t.unwrap());
            }
        }
    }

    fn scanToken(&mut self) -> Option<Token> {
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
            }
            '"' => {
                t = self.read_string();
            }
            ' ' | '\t' | '\n' => {
                // eat it
            }
            _ => {
                t = self.read_identifier();
            }
        }

        t
    }

    fn read_identifier(&mut self) -> Option<Token> {
        let mut c: char;
        while !self.is_at_end() {
            c = self.peek();
            match c {
                ' ' | '\t' | '[' | '(' | ')' | ']' => {
                    break;
                }
                _ => {}
            }
            self.advance();
        }

        let mut val: String = self.filter[self.start .. self.current].iter().collect();
        let mut tt: TokenType = TokenType::IDENTIFIER;

        match val.to_lowercase().as_str() {
            "and" | "not" | "or" => {
                val = val.to_lowercase();
                tt = TokenType::FILTER;
            }
            s => {
                if operators.get(s).is_some() {
                    val = val.to_lowercase();
                    tt = TokenType::OPERATOR;
                }
                else if s == "false" || s == "true" {
                    val = val.to_lowercase();
                    tt = TokenType::LITERAL;
                }
                else {
                    let mut chars = s.char_indices();
                    let (_, c) = chars.next().unwrap();
                    if c.is_ascii_digit() {
                        tt = TokenType::LITERAL;
                    }
                    else if c == '.' { // likely a decimal number or an attribute path
                        let next_char = chars.next();
                        if next_char.is_none() {
                            self.errors.push(format!("invalid identifier '{}' starting at position {}", &s, self.start));
                            return Option::None;
                        }

                        let (_, c) = next_char.unwrap();
                        if c.is_ascii_digit() {
                            tt = TokenType::LITERAL;
                        }
                        else if c == '_' || c.is_alphabetic() {
                            tt = TokenType::IDENTIFIER_PATH;
                        }
                        else {
                            self.errors.push(format!("invalid identifier '{}' starting at position {}", &s, self.start));
                            return Option::None;
                        }
                    }
                }
            }
        }

        Option::Some(Token { val, ttype: tt })
    }

    fn read_string(&mut self) -> Option<Token> {
        let mut prev: char = '"';
        let mut c: char = '\0';
        let mut val: Vec<char> = vec!();
        while !self.is_at_end() {
            c = self.peek();
            if c == '"' && prev != '\\' {
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

        if self.is_at_end() || c != '"' {
            let s: String = val.iter().collect();
            self.errors.push(format!("invalid string '{}' starting at position {}", s, self.start));
            return Option::None;
        }

        self.advance();

        Option::Some(Token { val: val.iter().collect(), ttype: TokenType::LITERAL })
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

    fn is_at_end(&self) -> bool {
        self.current >= self.len
    }
}

#[cfg(test)]
mod tests {
    use crate::scanner::scanTokens;
    use std::process::Command;

    struct FilterCandidate {
        filter: String,
        token_count: usize,
        error_count: usize
    }
    #[test]
    fn test_scaning() {
        let mut candidates: Vec<FilterCandidate> = vec!();
        let c1 = FilterCandidate{ filter: String::from("name eq \"abcd\""), token_count: 3, error_count: 0};
        candidates.push(c1);
        let c2 = FilterCandidate{ filter: String::from("not(name eq \"ab\\\"cd\")"), token_count: 6, error_count: 0};
        candidates.push(c2);
        let c3 = FilterCandidate{ filter: String::from("name eq \"abcd"), token_count: 0, error_count: 1};
        candidates.push(c3);
        let c4 = FilterCandidate{ filter: String::from("weight ge 0.7 and height le 20"), token_count: 7, error_count: 0};
        candidates.push(c4);
        let c5 = FilterCandidate{ filter: String::from("not(person[id eq 1].weight ge 0.7 and height le 20)"), token_count: 16, error_count: 0};
        candidates.push(c5);
        let c6 = FilterCandidate{ filter: String::from("not(person[id eq 1].weight ge 0.7 and (address.ishome eq false))"), token_count: 18, error_count: 0};
        candidates.push(c6);

        println!("begin scanning");
        for c in &candidates {
            let r = scanTokens(&c.filter);
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
        abnfgen.arg("-c").arg("search-filter.abnf");
        if abnfgen.output().is_err() {
            println!("abnfgen command failed, skipping fuzzing of filter scanner. Check the path of abnfgen and try again.");
            return;
        }

        let n = 2000;
        println!("testing scanner with {} generated filters", n);
        for i in (1..n) {
            let out = abnfgen.output().unwrap();
            let filter = String::from_utf8(out.stdout).unwrap();
            let filter = filter.replace("\n", "");
            let filter = filter.replace("\r", "");
            //println!("scanning: {}", &filter);
            let r = scanTokens(&filter);
            if r.is_err() {
                let se = r.err().unwrap();
                println!("{:?}\n{}", &se, filter);
            }
            assert!(true);
        }
    }
}