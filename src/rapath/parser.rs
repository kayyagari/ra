use std::borrow::Borrow;
use std::collections::VecDeque;
use std::rc::Rc;

use crate::errors::ParseError;
use crate::rapath::expr::{Ast, EvalFn, Operator};
use crate::rapath::expr::Ast::Literal;
use crate::rapath::functions::where_::where_;
use crate::rapath::scanner::{Token, TokenAndPos};
use crate::rapath::scanner::Token::*;
use crate::rapath::stypes::{Collection, SystemNumber, SystemString, SystemType};

struct Parser {
    tokens: VecDeque<TokenAndPos>
}

pub fn parse<'a>(mut tokens: VecDeque<TokenAndPos>) -> Result<Ast<'a>, ParseError> {
    let mut p = Parser{ tokens };

    p.parse()
}

impl<'a> Parser {
    fn parse(&mut self) -> Result<Ast<'a>, ParseError> {
        let e = self.expression(0)?;
        if self.peek().0 != EOF {
            return Err(ParseError::from_str("invalid expression, it was not completely parsed"));
        }
        Ok(e)
    }

    fn expression(&mut self, rbp: usize) -> Result<Ast<'a>, ParseError> {
        let mut left = self.null_denotation();
        while rbp < self.peek().0.lbp() {
            left = self.left_denotation(Box::new(left?));
        }
        left
    }

    fn null_denotation(&mut self) -> Result<Ast<'a>, ParseError> {
        let (t, pos) = self.advance();
        match t {
            LEFT_BRACE => {
                self.consume(&RIGHT_BRACE)?;
                let c: Collection = Collection::new_empty();
                Ok(Ast::Literal {val: Rc::new(SystemType::Collection(c))} )
            },
            TRUE => {
                Ok(Ast::Literal {val: Rc::new(SystemType::Boolean(true))})
            },
            FALSE => {
                Ok(Ast::Literal {val: Rc::new(SystemType::Boolean(false))})
            },
            STRING(s) => {
                Ok(Ast::Literal {val: Rc::new(SystemType::String(SystemString::new(s)))})
            },
            IDENTIFIER(id) => {
                Ok(Ast::Path {name: id})
            },
            CONSTANT(c) => {
                Ok(Ast::EnvVariable {val: SystemType::String(SystemString::new(c))})
            },
            NUMBER(n) => {
                // TODO separate integer and decimal
                // TODO handle quantity
                let sd = SystemNumber::from(&n)?;
                Ok(Ast::Literal {val: Rc::new(SystemType::Number(sd))})
            },
            LEFT_PAREN => {
                let e = self.expression(0)?;
                self.consume(&RIGHT_PAREN)?;
                Ok(e)
            },
            PLUS => {
                Ok(self.expression(0)?)
            },
            MINUS => {
                let mut e = self.expression(0)?;
                match e {
                    Ast::Literal{mut val} => {
                        if let SystemType::Number(ref n) = &*val {
                            Ok(Ast::Literal {val: Rc::new(SystemType::Number(n.to_negative_val()))})
                        }
                        else {
                            return Err(ParseError::new(format!("unary minus operator cannot be applied on a non-numeric value {:?}", &val)));
                        }
                    },
                    _ => {
                        Err(ParseError::new(format!("invalid token type {:?} for applying unary minus operator", t)))
                    }
                }
            }
            _ => {
                Err(ParseError::new(format!("unexpected token {}", t)))
            }
        }
    }

    fn left_denotation(&mut self, left: Box<Ast<'a>>) -> Result<Ast<'a>, ParseError> {
        let (t, pos) = self.advance();
        match t {
            DOT => {
                let rhs = self.expression(t.lbp())?;
                Ok(Ast::SubExpr {
                    lhs: left,
                    rhs: Box::new(rhs)
                })
            },
            AND => {
                let rhs = self.expression(t.lbp())?;
                Ok(Ast::Binary {
                    lhs: left,
                    rhs: Box::new(rhs),
                    op: Operator::And
                })
            },
            OR => {
                let rhs = self.expression(t.lbp())?;
                Ok(Ast::Binary {
                    lhs: left,
                    rhs: Box::new(rhs),
                    op: Operator::Or
                })
            },
            EQUAL => {
                let rhs = self.expression(t.lbp())?;
                Ok(Ast::Binary {
                    lhs: left,
                    rhs: Box::new(rhs),
                    op: Operator::Equal
                })
            },
            PLUS => {
                let rhs = self.expression(t.lbp())?;
                Ok(Ast::Binary {
                    lhs: left,
                    rhs: Box::new(rhs),
                    op: Operator::Plus
                })
            },
            LEFT_PAREN => match *left {
                Ast::Path {name: n, ..} => {
                    let args = self.parse_function_args()?;
                    let func: EvalFn = where_;
                    let f = Ast::Function {
                        name: n,
                        func,
                        args
                    };
                    Ok(f)
                }
                _ => {
                    Err(ParseError::new(format!("invalid function name {}", t)))
                }
            }
            _ => {
                Err(ParseError::new(format!("unexpected token on rhs {}", t)))
            }
        }
    }

    fn parse_function_args(&mut self) -> Result<Vec<Ast<'a>>, ParseError> {
        let mut args = Vec::new();

        while self.peek().0 != RIGHT_PAREN {
            let e = self.expression(0)?;
            args.push(e);

            if self.peek().0 == COMMA {
                self.advance();
                if self.peek().0 != RIGHT_PAREN {
                    return Err(ParseError::from_str("invalid trailing comma in function arguments"));
                }
            }
        }
        self.advance();
        Ok(args)
    }

    #[inline]
    fn consume(&mut self, tt: &Token) -> Result<TokenAndPos, ParseError> {
        if self.check(tt) {
            return Ok(self.advance());
        }
        let (found, pos) = self.peek();
        Err(ParseError::new(format!("expected token {} but found {}", &tt, found)))
    }

    #[inline]
    fn peek(&self) -> &(Token, usize) {
        self.peek_at(0)
    }

    #[inline]
    fn peek_at(&self, index: usize) -> &(Token, usize) {
        if let Some(t) = self.tokens.get(index) {
            return t;
        }

        &(Token::EOF, 1)
    }

    #[inline]
    fn is_at_end(&self) -> bool {
        self.peek().0 == EOF
    }

    #[inline]
    fn match_tt(&mut self, tt: Token) -> bool {
        let found = &self.peek().0;
        if found == &Token::EOF {
            return false;
        }

        if found == &tt {
            self.tokens.pop_front();
            return true;
        }

        false
    }

    #[inline]
    fn check(&self, tt: &Token) -> bool {
        if self.is_at_end() {
            return false;
        }

        &self.peek().0 == tt
    }

    #[inline]
    fn advance(&mut self) -> (Token, usize) {
        if let Some(t) = self.tokens.pop_front() {
            return t;
        }

        (Token::EOF, 1)
    }
}

#[cfg(test)]
mod tests {
    use crate::rapath::parser::parse;
    use crate::rapath::scanner::{scan_tokens, Token};

    struct ExprCandidate<'a> {
        e: &'a str,
        valid: bool
    }

    #[test]
    fn test_simple_expr() {
        let mut xprs = vec!();
        let x1 = ExprCandidate{e: "1+1", valid: true};
        xprs.push(x1);

        let x2 = ExprCandidate{e: "1+1 and 0 + 6", valid: true};
        xprs.push(x2);

        let x3 = ExprCandidate{e: "(1+1)", valid: true};
        xprs.push(x3);

        let x4 = ExprCandidate{e: "Patient.name.first(1+1)", valid: true};
        xprs.push(x4);

        let x5 = ExprCandidate{e: "+1", valid: true};
        xprs.push(x5);

        let x6 = ExprCandidate{e: "-1", valid: true};
        xprs.push(x6);

        let x6 = ExprCandidate{e: "1-", valid: false};
        xprs.push(x6);

        for x in xprs {
            let tokens = scan_tokens(&String::from(x.e)).unwrap();
            // println!("{:?}", &tokens);
            let result = parse(tokens);
            if x.valid {
                assert!(result.is_ok());
                // println!("{:?}", result.unwrap());
            }
            else {
                println!("{:?}", result.as_ref().err().unwrap());
                assert!(!x.valid);
            }
        }
    }
}