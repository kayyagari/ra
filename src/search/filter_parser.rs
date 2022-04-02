use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::errors::ParseError;
use crate::search::{ComparisonOperator, Filter};
use crate::search::FilterType::*;
use crate::search::filter_scanner::*;
use crate::search::filter_scanner::TokenType::*;

struct Parser {
    tokens: Vec<Token>,
    current: usize,
    open_paren_count: i32,
    open_bracket_count: i32
}

pub fn parse(mut tokens: Vec<Token>) -> Result<Filter, ParseError> {
    let mut p = Parser{ tokens, current: 0, open_paren_count: 0, open_bracket_count: 0};
    p.parse()
}

impl Parser {
    fn parse(&mut self) -> Result<Filter, ParseError> {
        let mut e: Option<Filter> = None;
        while !self.is_at_end() {
            let t = self.peek();
            match t.ttype {
                IDENTIFIER => {
                    if e.is_some() {
                        let prev_type = e.as_ref().unwrap().get_type();
                        if prev_type == Simple || prev_type == Conditional {
                            return Err(ParseError::new(format!("invalid filter, two or more simple expressions must be bound by a logical expression")));
                        }
                    }
                    e = Some(self.parse_expr()?);
                },
                LEFT_PAREN => {
                    self.advance();
                    self.open_paren_count += 1;
                    e = Some(self.parse()?);
                    self.consume(TokenType::RIGHT_PAREN)?;
                    self.open_paren_count -= 1;
                },
                LOGIC_OPERATOR => {
                    let op = t.val.as_str();
                    match op {
                        "and" => {
                            if e.is_none() {
                                return Err(ParseError::new(format!("invalid AND expression in filter")));
                            }
                            let mut children = vec![Box::new(e.unwrap())];
                            self.advance();
                            let rhs = self.parse()?;
                            children.push(Box::new(rhs));
                            e = Some(Filter::AndFilter { children });
                        },
                        "not" => {
                            if e.is_some() {
                                let prev_type = e.as_ref().unwrap().get_type();
                                if prev_type != And || prev_type != Or {
                                    return Err(ParseError::from_str("misplaced NOT expression in filter"));
                                }
                            }
                            self.advance();
                            let rhs = self.parse()?;
                            let not = Filter::NotFilter {child: Box::new(rhs)};
                            e = Some(not);
                        },
                        "or" => {
                            if e.is_none() {
                                return Err(ParseError::new(format!("invalid OR expression in filter")));
                            }
                            let mut children = vec![Box::new(e.unwrap())];
                            self.advance();
                            let rhs = self.parse()?;
                            children.push(Box::new(rhs));
                            e = Some(Filter::OrFilter {children});
                        },
                        s => {
                            return Err(ParseError::new(format!("invalid filter expression, found {}", s)));
                        }
                    }
                },
                RIGHT_PAREN => {
                    if self.open_paren_count - 1 < 0 {
                        return Err(ParseError::new(format!("invalid closing {}", RIGHT_PAREN)));
                    }
                    break;
                },
                RIGHT_BRACKET => {
                    if self.open_bracket_count - 1 < 0 {
                        return Err(ParseError::new(format!("invalid closing {}", RIGHT_BRACKET)));
                    }
                    break;
                },
                t => {
                    return Err(ParseError::new(format!("invalid token type {}", t)));
                }
            }

        }

        if e.is_none() {
            return Err(ParseError::from_str("invalid filter"));
        }

        Ok(e.unwrap())
    }

    fn parse_expr(&mut self) -> Result<Filter, ParseError> {
        let id = self.consume(IDENTIFIER)?;
        let id = id.val.clone();

        let mut cond_expr: Option<Filter> = None;
        let mut id_path: Option<String> = None;
        if self.peek().ttype == TokenType::LEFT_BRACKET { // there is a conditional expression
            self.consume(TokenType::LEFT_BRACKET)?;
            self.open_bracket_count += 1;
            let ce = self.parse()?;
            self.consume(TokenType::RIGHT_BRACKET)?;
            let id_path_token = self.consume(TokenType::IDENTIFIER_PATH)?;
            id_path = Some(id_path_token.val.clone());
            self.open_bracket_count -= 1;
            cond_expr = Some(ce);
        }

        let op = self.consume(COMPARISON_OPERATOR)?;
        let op = OPERATORS.get(op.val.as_str()).unwrap();
        let va = self.consume(LITERAL)?;

        if cond_expr.is_some() {
            let condition = Box::new(cond_expr.unwrap());
            let ce = Filter::ConditionalFilter {identifier: id, id_path: id_path.unwrap(), operator: *op, value: va.val.clone(), condition};
            return Ok(ce);
        }

        let se = Filter::StringFilter {identifier: id, operator: *op, value: va.val.clone()};
        Ok(se)
    }

    fn consume(&mut self, tt: TokenType) -> Result<&Token, ParseError> {
        if self.check(tt) {
            return Ok(self.advance());
        }
        let found = self.peek();
        Err(ParseError::new(format!("expected token {} but found {} with value {}", tt, found.ttype, &found.val)))
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.current]
    }

    fn is_at_end(&self) -> bool {
        self.peek().ttype == TokenType::EOF
    }

    fn check(&self, tt: TokenType) -> bool {
        if self.is_at_end() {
            return false;
        }

        self.peek().ttype == tt
    }

    fn advance(&mut self) -> &Token {
        if !self.is_at_end() {
            self.current += 1;
        }

        self.previous()
    }

    fn previous(&self) -> &Token {
        &self.tokens[self.current - 1]
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use crate::errors::ParseError;
    use crate::search::Filter;
    use crate::search::filter_parser::parse;
    use crate::search::filter_scanner::scan_tokens;

    fn parse_filter(filter: &String) -> Result<Filter, ParseError> {
        let tokens = scan_tokens(&filter).expect("failed to scan the filter");
        parse(tokens)
    }

    struct FilterCandidate {
        input: String,
        output: String,
        success: bool
    }

    #[test]
    fn test_parse_valid() {
        let mut filters = vec!();
        let f1 = FilterCandidate{ input: String::from("name eq \"abcd\""), output: String::from("(name EQ abcd)"), success: true};
        filters.push(f1);

        let f2 = FilterCandidate{ input: String::from("name eq \"abcd\" and age gt 25"), success: true, output: String::from("((name EQ abcd) AND (age GT 25))")};
        filters.push(f2);

        // within parentheses
        let f3 = FilterCandidate{ input: String::from("(name eq \"abcd\")"), success: true, output: String::from("(name EQ abcd)")};
        filters.push(f3);

        // within parentheses
        let f4 = FilterCandidate{ input: String::from("((name EQ \"abcd\") AND (age GT 25))"), success: true, output: String::from("((name EQ abcd) AND (age GT 25))")};
        filters.push(f4);

        // conditional expression
        let f5 = FilterCandidate{ input: String::from("(name[given eq \"A\"].last co \"abcd\")"), success: true, output: String::from("(name[(given EQ A)].last CO abcd)")};
        filters.push(f5);

        let f6 = FilterCandidate{ input: String::from("not(name eq \"abcd\")"), success: true, output: String::from("NOT(name EQ abcd)")};
        filters.push(f6);

        let f7 = FilterCandidate{ input: String::from("age gt 25 and not(name eq \"abcd\")"), success: true, output: String::from("((age GT 25) AND NOT(name EQ abcd))")};
        filters.push(f7);

        for f in filters {
            let expr = parse_filter(&f.input);
            assert!(f.success);
            let expr = expr.unwrap();
            assert_eq!(f.output, expr.to_string());
        }
    }

    #[test]
    /// tests that are valid for scanner but not for parser
    fn test_parse_invalid() {
        let mut filters = vec!();
        filters.push(String::from("name eq \"abcd\" age"));
        filters.push(String::from("(name eq \"abcd\""));
        filters.push(String::from("name eq \"abcd\")"));
        filters.push(String::from("(name eq \"abcd\"))"));
        filters.push(String::from("name[z eq 1.a eq \"abcd\""));
        filters.push(String::from("namez eq 1].a eq \"abcd\""));

        // logical errors
        filters.push(String::from("name eq \"abcd\" not(age gt 25)"));
        filters.push(String::from("name eq \"abcd\" age gt 25"));
        filters.push(String::from("and and"));
        filters.push(String::from("age gt 25 and and"));
        filters.push(String::from("or or"));
        filters.push(String::from("age gt 25 or or"));
        filters.push(String::from("age gt 25 or or[]"));
        filters.push(String::from("_n1-_U70KQ8w[not(NOT(NoT(Not(nOT(J3 sw \"a\")))and))].Vb[noT(z5t6yk9x4[R20A274 GE 2].oF sa S)].gW CO \"[0SZWC\""));
        for f in filters {
            let r = parse_filter(&f);
            let x = r.as_ref().err().unwrap();
            println!("{:?}", x);
            assert!(r.is_err());
        }
    }

    #[test]
    fn test_using_abnfgen() {
        let mut abnfgen = Command::new("abnfgen");
        abnfgen.arg("-c").arg("search-filter.abnf");
        if abnfgen.output().is_err() {
            println!("abnfgen command failed, skipping fuzzing of filter parser. Check the path of abnfgen and try again.");
            return;
        }

        let n = 200;
        println!("testing parser with {} generated filters", n);
        for _ in 1..n {
            let out = abnfgen.output().unwrap();
            let filter = String::from_utf8(out.stdout).unwrap();
            let filter = filter.replace("\n", "");
            let filter = filter.replace("\r", "");
            let tokens = scan_tokens(&filter);
            if tokens.is_err() {
                // no reason to parse further if scanner found errors
                continue;
            }
            //println!("parsing: {}", &filter);
            let r = parse(tokens.unwrap());
            if r.is_err() {
                assert!(r.is_err());
                let pe = r.err().unwrap();
                println!("{:?}\n{}", &pe, filter);
            }
            else {
                assert!(r.is_ok());
            }
        }
    }
}