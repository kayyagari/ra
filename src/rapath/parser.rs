use crate::parser::ParseError;
use crate::rapath::expr::{Ast, Operator, Collection, SystemType, SystemNumber};
use crate::rapath::scanner::{TokenAndPos, Token};
use crate::rapath::scanner::Token::*;
use std::collections::VecDeque;
use crate::rapath::expr::Ast::Literal;

struct Parser {
    tokens: VecDeque<TokenAndPos>
}

pub fn parse(mut tokens: VecDeque<TokenAndPos>) -> Result<Ast, ParseError> {
    let mut p = Parser{ tokens };

    p.parse()
}

impl Parser {
    fn parse(&mut self) -> Result<Ast, ParseError> {
        let e = self.expression(0)?;
        if self.peek().0 != EOF {
            return Err(ParseError{msg: String::from("invalid expression, it was not completely parsed")});
        }
        Ok(e)
    }

    fn expression(&mut self, rbp: usize) -> Result<Ast, ParseError> {
        let mut left = self.null_denotation();
        while rbp < self.peek().0.lbp() {
            left = self.left_denotation(Box::new(left?));
        }
        left
    }

    fn null_denotation(&mut self) -> Result<Ast, ParseError> {
        let (t, pos) = self.advance();
        match t {
            LEFT_BRACE => {
                self.consume(&RIGHT_BRACE)?;
                let c: Collection<SystemType> = Collection::new();
                Ok(Ast::Literal {val: SystemType::Collection(c)} )
            },
            TRUE => {
                Ok(Ast::Literal {val: SystemType::Boolean(true)})
            },
            FALSE => {
                Ok(Ast::Literal {val: SystemType::Boolean(false)})
            },
            STRING(s) => {
                Ok(Ast::Literal {val: SystemType::String(s)})
            },
            IDENTIFIER(id) => {
                Ok(Ast::Identifier {name: id})
            },
            CONSTANT(c) => {
                Ok(Ast::EnvVariable {val: SystemType::String(c)})
            },
            NUMBER(n) => {
                // TODO separate integer and decimal
                // TODO handle quantity
                let sd = SystemNumber::from(&n)?;
                Ok(Ast::Literal {val: SystemType::Number(sd)})
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
                    Ast::Literal{val: ref mut v} => {
                        if let SystemType::Number(ref mut n) = v {
                            n.to_negative_val();
                            Ok(e)
                        }
                        else {
                            return Err(ParseError{msg: format!("unary minus operator cannot be applied on a non-numeric value {:?}", &v)});
                        }
                    },
                    _ => {
                        Err(ParseError{msg: format!("invalid token type {:?} for applying - operator", t)})
                    }
                }
            }
            _ => {
                Err(ParseError{msg: format!("unexpected token {}", t)})
            }
        }
    }

    fn left_denotation(&mut self, left: Box<Ast>) -> Result<Ast, ParseError> {
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
                Ast::Identifier {name: n, ..} => {
                    let args = self.parse_function_args()?;
                    let f = Ast::Function {
                        name: n,
                        args
                    };
                    Ok(f)
                }
                _ => {
                    Err(ParseError{msg: format!("invalid function name {}", t)})
                }
            }
            _ => {
                Err(ParseError{msg: format!("unexpected token on rhs {}", t)})
            }
        }
    }

    fn parse_function_args(&mut self) -> Result<Vec<Ast>, ParseError> {
        let mut args = Vec::new();

        while self.peek().0 != RIGHT_PAREN {
            let e = self.expression(0)?;
            args.push(e);

            if self.peek().0 == COMMA {
                self.advance();
                if self.peek().0 != RIGHT_PAREN {
                    return Err(ParseError{msg: String::from("invalid trailing comma in function arguments")});
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
        Err(ParseError{msg: format!("expected token {} but found {}", &tt, found)})
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
    use crate::rapath::parser::{parse};
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
                println!("{:?}", result.unwrap());
            }
            else {
                println!("{:?}", result.as_ref().err().unwrap());
                assert!(!x.valid);
            }
        }
    }
}