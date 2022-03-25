use std::borrow::Borrow;
use std::collections::VecDeque;
use std::rc::Rc;

use crate::errors::ParseError;
use crate::rapath::expr::{Ast, Function, Operator};
use crate::rapath::expr::Ast::Literal;
use crate::rapath::functions::where_::{where_};
use crate::rapath::scanner::{Token, TokenAndPos};
use crate::rapath::scanner::Token::*;
use crate::rapath::stypes::{Collection, SystemDateTime, SystemNumber, SystemQuantity, SystemString, SystemTime, SystemType};
use crate::res_schema::SchemaDef;

struct Parser<'b> {
    tokens: VecDeque<TokenAndPos>,
    sd: Option<&'b SchemaDef>,
    // a dequeue that holds true if the current token is DOT and false otherwise for each token
    // this is to assist in stripping the resource name from the expressions
    prev: VecDeque<bool>
}

pub fn parse<'a>(mut tokens: VecDeque<TokenAndPos>) -> Result<Ast<'a>, ParseError> {
    let mut p = Parser{ tokens, sd: None, prev: VecDeque::new() };

    p.parse()
}

pub fn parse_with_schema(mut tokens: VecDeque<TokenAndPos>, sd: Option<&SchemaDef>) -> Result<Ast, ParseError> {
    let mut p = Parser{ tokens, sd, prev: VecDeque::new() };

    p.parse()
}

impl<'a> Parser<'a> {
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
                // strip the resourcename if present in the beginning of the identifier
                if !self.is_prev_dot() {
                    if self.is_resource_name(&id) {
                        // proceed only if the next token is DOT
                        // otherwise it will wrongly assumes that an expression like "where(resolve() is Patient)"
                        // has a path after the resource name "Patient"
                        if self.peek().0 == DOT {
                            self.advance();
                            return self.null_denotation();
                        }
                    }
                }
                Ok(Ast::Path {name: id})
            },
            CONSTANT(c) => {
                Ok(Ast::EnvVariable {name: c})
            },
            NUMBER(n) => {
                let sd = SystemNumber::from(&n)?;
                Ok(Ast::Literal {val: Rc::new(SystemType::Number(sd))})
            },
            DATE_TIME(dt) => {
                Ok(Ast::Literal {val: Rc::new(SystemType::DateTime(dt))})
            },
            TIME(t) => {
                Ok(Ast::Literal {val: Rc::new(SystemType::Time(t))})
            },
            QUANTITY(val, unit) => {
                Ok(Ast::Literal {val: Rc::new(SystemType::Quantity(SystemQuantity::new(val, unit)))})
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
            },
            DOLLAR_THIS => {
                Ok(Ast::Variable {name: String::from("$this")})
            },
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
            AND | OR | XOR | EQUAL | NOT_EQUAL | EQUIVALENT | NOT_EQUIVALENT |
            PLUS | GREATER | GREATER_EQUAL | LESS | LESS_EQUAL | IMPLIES | UNION => {
                let op = match t {
                    AND => Operator::And,
                    OR => Operator::Or,
                    XOR => Operator::Xor,
                    EQUAL => Operator::Equal,
                    NOT_EQUAL => Operator::NotEqual,
                    EQUIVALENT => Operator::Equivalent,
                    NOT_EQUIVALENT => Operator::NotEquivalent,
                    PLUS => Operator::Plus,
                    GREATER => Operator::Greater,
                    GREATER_EQUAL => Operator::GreaterEqual,
                    LESS => Operator::Less,
                    LESS_EQUAL => Operator::LessEqual,
                    IMPLIES => Operator::Implies,
                    UNION => Operator::Union,
                    _ => Operator::Greater // never happens, but to keep compiler happy
                };
                let rhs = self.expression(t.lbp())?;
                Ok(Ast::Binary {
                    lhs: left,
                    rhs: Box::new(rhs),
                    op
                })
            },
            IS => {
                // if the RHS is a Path (created from an IDENTIFIER token)
                // then convert it into a String literal
                let mut rhs = self.expression(t.lbp())?;
                if let Ast::Path{name} = rhs {
                    rhs = Ast::Literal {val: Rc::new(SystemType::String(SystemString::new(name)))};
                }

                Ok(Ast::Binary {
                    lhs: left,
                    rhs: Box::new(rhs),
                    op: Operator::Is
                })
            },
            AS => {
                // if the RHS is a Path (created from an IDENTIFIER token)
                // then convert it into a String literal
                let rhs = self.expression(t.lbp())?;
                let type_name;
                if let Ast::Path{name} = rhs {
                    type_name = name;
                }
                else {
                    return Err(ParseError::new(format!("invalid type name in AS expression",)))
                }

                match *left {
                    Ast::Path { name} => {
                        let at_and_type_name = format!("{}{}", &name, &type_name);
                        return Ok(Ast::TypeCast {at_name: name, type_name, at_and_type_name});
                    },
                    Ast::SubExpr {lhs: prev_lhs, rhs: prev_rhs} => {
                        if let Ast::Path { name} = &*prev_rhs {
                            let at_and_type_name = format!("{}{}", name, &type_name);
                            let cast = Ast::TypeCast {at_name: name.clone(), type_name, at_and_type_name};
                            return Ok(Ast::SubExpr {lhs: prev_lhs, rhs: Box::new(cast)});
                        }
                    },
                    _ => {}
                }
                Err(ParseError::new(format!("invalid lhs in AS expression")))
            },
            LEFT_PAREN => match *left {
                Ast::Path {name: n, ..} => {
                    let args = self.parse_function_args()?;
                    // let func: Box<dyn EvalFunc> = Box::new(WhereFunction::new(args));
                    let func: Function = Function::NameAndArgs(n, args);
                    let f = Ast::Function {
                        // name: n,
                        func,
                        // args
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
            self.prev.push_front(t.0 == DOT);
            return t;
        }

        self.prev.push_front(false);
        (Token::EOF, 1)
    }

    #[inline]
    fn is_resource_name(&self, id: &String) -> bool {
        let mut found = false;
        if let Some(sd) = self.sd {
            found = sd.resources.contains_key(id);
        }
        found
    }

    #[inline]
    fn is_prev_dot(&self) -> bool {
        let mut dot = false;
        if let Some(t) = self.prev.get(1) {
            dot = *t;
        }
        dot
    }
}

#[cfg(test)]
mod tests {
    use crate::rapath::parser::parse;
    use crate::rapath::scanner::{scan_tokens, Token};

    #[test]
    fn test_simple_expr() {
        let mut xprs = vec!();
        xprs.push(("1+1", true));
        xprs.push(("1+1 and 0 + 6", true));
        xprs.push(("(1+1)", true));
        xprs.push(("Patient.name.first(1+1)", true));
        xprs.push(("+1", true));
        xprs.push(("-1", true));
        xprs.push(("1-", false));
        xprs.push(("1 > 1", true));
        xprs.push(("1 >= 1", true));
        xprs.push(("1 < 1", true));
        xprs.push(("1 <= 1", true));

        for (expr, expected) in xprs {
            let tokens = scan_tokens(expr).unwrap();
            // println!("{:?}", &tokens);
            let result = parse(tokens);
            assert_eq!(expected, result.is_ok());
        }
    }

    #[test]
    fn test_parsing_as_expr() {
        let mut exprs = Vec::new();
        exprs.push("DeviceRequest.code as CodeableConcept");
        exprs.push("code as CodeableConcept");
        for input in exprs {
            let tokens = scan_tokens(input).unwrap();
            let result = parse(tokens);
            assert!(result.is_ok());
        }

        let result = parse(scan_tokens("as CodeableConcept").unwrap());
        assert!(result.is_err());
    }
}