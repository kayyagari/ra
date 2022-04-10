use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::iter::Peekable;
use std::str::CharIndices;

use lazy_static::lazy_static;

use crate::errors::ScanError;
use crate::search::ComparisonOperator;

lazy_static! {
 pub static ref OPERATORS: HashMap<&'static str, ComparisonOperator> = {
        let mut ops = HashMap::new();
        ops.insert("eq", ComparisonOperator::EQ);
        ops.insert("ne", ComparisonOperator::NE);
        ops.insert("co", ComparisonOperator::CO);
        ops.insert("sw", ComparisonOperator::SW);
        ops.insert("ew", ComparisonOperator::EW);
        ops.insert("gt", ComparisonOperator::GT);
        ops.insert("lt", ComparisonOperator::LT);
        ops.insert("ge", ComparisonOperator::GE);
        ops.insert("le", ComparisonOperator::LE);
        ops.insert("ap", ComparisonOperator::AP);
        ops.insert("sa", ComparisonOperator::SA);
        ops.insert("eb", ComparisonOperator::EB);
        ops.insert("pr", ComparisonOperator::PR);
        ops.insert("po", ComparisonOperator::PO);
        ops.insert("ss", ComparisonOperator::SS);
        ops.insert("sb", ComparisonOperator::SB);
        ops.insert("in", ComparisonOperator::IN);
        ops.insert("ni", ComparisonOperator::NI);
        ops.insert("re", ComparisonOperator::RE);
        ops
    };
}

#[derive(Debug)]
struct Scanner<'a> {
    filter: Peekable<CharIndices<'a>>,
    errors: Vec<String>
}

#[derive(Debug)]
pub struct Token {
    pub val: String,
    pub ttype: TokenType,
}

#[derive(Debug, PartialEq, Copy, Clone)]
#[allow(non_camel_case_types)]
pub enum TokenType {
    LEFT_PAREN,
    RIGHT_PAREN,
    LEFT_BRACKET,
    RIGHT_BRACKET,
    LITERAL,
    IDENTIFIER,
    COMPARISON_OPERATOR,
    LOGIC_OPERATOR,
    IDENTIFIER_PATH,
    EOF
}

pub fn scan_tokens(filter: &str) -> Result<Vec<Token>, ScanError> {
    let mut scanner = Scanner {
        filter: filter.char_indices().peekable(),
        errors: vec!(),
    };

    let mut tokens: Vec<Token> = vec!();
    scanner.scan(&mut tokens);
    if !scanner.errors.is_empty() {
        return Err(ScanError{errors: scanner.errors});
    }

    let eof = Token{ val: String::from(""), ttype: TokenType::EOF};
    tokens.push(eof);

    Ok(tokens)
}

impl Display for TokenType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s;
        match self {
            TokenType::COMPARISON_OPERATOR => {
                s = "comparison operator";
            },
            TokenType::LITERAL => {
                s = "literal";
            },
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
            TokenType::LOGIC_OPERATOR => {
                s = "logical operator";
            },
            TokenType::IDENTIFIER_PATH => {
                s = "identifier path";
            },
            TokenType::EOF => {
                s = "EOF";
            }
        }

        f.write_str(s)
    }
}

impl Scanner<'_> {
    fn scan(&mut self, tokens: &mut Vec<Token>) {
        loop {
            match self.filter.next() {
                Some((pos, c)) => {
                    match c {
                        '(' => {
                            tokens.push(Token { val: String::from('('), ttype: TokenType::LEFT_PAREN });
                        }
                        ')' => {
                            tokens.push(Token { val: String::from(')'), ttype: TokenType::RIGHT_PAREN });
                        }
                        '[' => {
                            tokens.push(Token { val: String::from('['), ttype: TokenType::LEFT_BRACKET });
                        }
                        ']' => {
                            tokens.push(Token { val: String::from(']'), ttype: TokenType::RIGHT_BRACKET });
                        }
                        '"' => {
                            let t = self.read_string(pos);
                            if let Some(t) = t {
                                tokens.push(t);
                            }
                        }
                        ' ' | '\t' | '\n' => {
                            // eat it
                        }
                        _ => {
                            let t = self.read_identifier(c, pos);
                            if let Some(t) = t {
                                tokens.push(t);
                            }
                        }
                    }
                },
                None => {
                    break;
                }
            }
        }
    }

    fn read_identifier(&mut self, start: char, pos: usize) -> Option<Token> {
        let mut val = String::with_capacity(16);
        val.push(start);
        loop {
            match self.filter.peek() {
                Some((pos, c)) => {
                    match c {
                        ' ' | '\t' | '[' | '(' | ')' | ']' => {
                            break;
                        }
                        _ => {
                            val.push(*c);
                            self.filter.next();
                        }
                    }
                },
                None => { break; }
            }
        }

        let mut tt: TokenType = TokenType::IDENTIFIER;

        match val.to_lowercase().as_str() {
            "and" | "not" | "or" => {
                val = val.to_lowercase();
                tt = TokenType::LOGIC_OPERATOR;
            }
            s => {
                if OPERATORS.get(s).is_some() {
                    val = val.to_lowercase();
                    tt = TokenType::COMPARISON_OPERATOR;
                } else if s == "false" || s == "true" {
                    val = val.to_lowercase();
                    tt = TokenType::LITERAL;
                } else {
                    let mut chars = s.char_indices();
                    let (_, c) = chars.next().unwrap();
                    if c.is_ascii_digit() {
                        tt = TokenType::LITERAL;
                    } else if c == '.' { // likely a decimal number or an attribute path
                        let next_char = chars.next();
                        if next_char.is_none() {
                            self.errors.push(format!("invalid identifier '{}' starting at position {}", &s, pos));
                            return None;
                        }

                        let (_, c) = next_char.unwrap();
                        if c.is_ascii_digit() {
                            tt = TokenType::LITERAL;
                        } else if c == '_' || c.is_alphabetic() {
                            tt = TokenType::IDENTIFIER_PATH;
                        } else {
                            self.errors.push(format!("invalid identifier '{}' starting at position {}", &s, pos));
                            return None;
                        }
                    }
                }
            }
        }

        Some(Token { val, ttype: tt })
    }

    fn read_string(&mut self, start: usize) -> Option<Token> {
        let mut prev: char = '"';
        let mut s = String::with_capacity(16);
        loop {
            match self.filter.next() {
                Some((pos, c)) => {
                    if c == '"' && prev != '\\' {
                        break;
                    }
                    if prev == '\\' {
                        s.pop(); // remove the \
                        match c {
                            'r' => s.push('\r'),
                            'n' => s.push('\n'),
                            't' => s.push('\t'),
                            'f' => s.push_str("\\f"),
                            ',' => s.push_str("\\,"),
                            '$' => s.push_str("\\$"),
                            '|' => s.push_str("\\|"),
                            _ => {
                                s.push(c);
                            }
                        }
                    } else {
                        s.push(c);
                    }
                    prev = c;
                },
                None => {
                    self.errors.push(format!("invalid string '{}' starting at position {}", &s, start));
                    break;
                }
            }
        }
        Some(Token { val: s, ttype: TokenType::LITERAL })
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use crate::search::filter_scanner::scan_tokens;

    #[test]
    fn test_scaning() {
        let mut candidates = vec!();
        candidates.push(("name eq \"abcd\"", 3, 0));
        candidates.push(("not(name eq \"ab\\\"cd\")", 6, 0));
        candidates.push(("name eq \"abcd", 0, 1));
        candidates.push(("name eq \"ab,c\\,d\"", 3, 0));
        candidates.push(("weight ge 0.7 and height le 20", 7, 0));
        candidates.push(("not(person[id eq 1].weight ge 0.7 and height le 20)", 16, 0));
        candidates.push(("not(person[id eq 1].weight ge 0.7 and (address.ishome eq false))", 18, 0));

        println!("begin scanning");
        for (input, token_count, error_count) in candidates {
            let r = scan_tokens(input);
            if r.is_ok() {
                let tokens = r.as_ref().unwrap();
                println!("{:?}", &tokens);
            }
            else {
                let se = r.as_ref().err().unwrap();
                println!("{:?}", &se);
            }
            if error_count != 0 {
                let se = r.err().unwrap();
                assert_eq!(error_count, se.errors.len());
            }
            else {
                assert!(r.is_ok());
                let tokens = r.unwrap();
                assert_eq!(token_count, tokens.len() - 1); // excluding the EOF token
            }
        }

        // test escaping of , $ and | chars
        let mut candidates = vec!();
        candidates.push(("name eq \"ab,c\\,d\"", "ab,c\\,d"));
        candidates.push(("name eq \"ab$c\\$d\"", "ab$c\\$d"));
        candidates.push(("name eq \"ab|c\\|d\"", "ab|c\\|d"));
        candidates.push(("name:exact eq \"ab|c\\|d\"", "ab|c\\|d"));
        for (input, expected_val) in candidates {
            let r = scan_tokens(input).unwrap();
            assert_eq!(expected_val, r[2].val);
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