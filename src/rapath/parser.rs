use crate::parser::ParseError;
use crate::rapath::expr::{Ast, Operator, Collection, SystemType, SystemNumber};
use crate::rapath::scanner::{Token, TokenType};
use crate::rapath::scanner::TokenType::*;

struct Parser {
    tokens: Vec<Token>,
    current: usize,
    open_paren_count: i32,
    open_bracket_count: i32
}

pub fn parse(mut tokens: Vec<Token>) -> Result<Ast, ParseError> {
    let eof = Token{ val: String::from(""), ttype: TokenType::EOF};
    tokens.push(eof);

    let mut p = Parser{ tokens, current: 0, open_paren_count: 0, open_bracket_count: 0};

    p.parse()
}

impl Parser {
    fn parse(&mut self) -> Result<Ast, ParseError> {
        let e = self.expression(0)?;
        if self.peek().ttype != EOF {
            return Err(ParseError{msg: String::from("invalid expression, it was not completely parsed")});
        }
        Ok(e)
    }

    fn expression(&mut self, rbp: usize) -> Result<Ast, ParseError> {
        let mut left = self.null_denotation();
        while rbp < self.peek().ttype.lbp() {
            left = self.left_denotation(Box::new(left?));
        }
        left
    }

    fn null_denotation(&mut self) -> Result<Ast, ParseError> {
        let t = self.advance().ttype;
        match t {
            LEFT_BRACE => {
                self.consume(RIGHT_BRACE)?;
                let c: Collection<SystemType> = Collection::new();
                Ok(Ast::Literal {val: SystemType::Collection(c)} )
            },
            TRUE => {
                Ok(Ast::Literal {val: SystemType::Boolean(true)})
            },
            FALSE => {
                Ok(Ast::Literal {val: SystemType::Boolean(false)})
            },
            STRING | IDENTIFIER => {
                Ok(Ast::Literal {val: SystemType::String(self.previous().val.clone())})
            },
            CONSTANT => {
                Ok(Ast::EnvVariable {val: SystemType::String(self.previous().val.clone())})
            },
            NUMBER => {
                // TODO separate integer and decimal
                // TODO handle quantity
                let sd = SystemNumber::from(&self.previous().val)?;
                Ok(Ast::Literal {val: SystemType::Number(sd)})
            },
            LEFT_PAREN => {
              let e = self.expression(0)?;
                self.consume(RIGHT_PAREN)?;
                Ok(e)
            },

            _ => {
                Err(ParseError{msg: format!("unexpected token {} {}", t, &self.previous().val)})
            }
        }
    }

    fn left_denotation(&mut self, left: Box<Ast>) -> Result<Ast, ParseError> {
        let t = self.advance().ttype;
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
            _ => {
                Err(ParseError{msg: format!("unexpected token on rhs {}", t)})
            }
        }
    }

    // fn function(&mut self) -> Result<Option<Ast>, ParseError> {
    //     if self.match_tt(IDENTIFIER) {
    //         let name = self.previous().val.clone();
    //         self.consume(LEFT_PAREN);
    //         let mut params: Vec<Ast> = vec!();
    //         while !self.match_tt(RIGHT_PAREN) {
    //             let e = self.expression()?;
    //             params.push(e);
    //             if self.match_tt(COMMA) {
    //                 self.advance();
    //             }
    //         }
    //         self.consume(RIGHT_PAREN);
    //
    //         let f = FunctionExpr{ name, params};
    //         return Ok(Option::Some(Box::new(f)));
    //     }
    //
    //     Ok(Option::None)
    // }

    fn consume(&mut self, tt: TokenType) -> Result<&Token, ParseError> {
        if self.check(tt) {
            return Ok(self.advance());
        }
        let found = self.peek();
        Err(ParseError{msg: format!("expected token {} but found {} with value {}", tt, found.ttype, &found.val)})
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.current]
    }

    fn peek_double(&self) -> &Token {
        let n = self.current + 1;
        if n >= self.tokens.len() {
            return &self.tokens[self.current];
        }

        &self.tokens[n]
    }

    fn is_at_end(&self) -> bool {
        self.peek().ttype == TokenType::EOF
    }

    fn match_tt(&mut self, tt: TokenType) -> bool {
        let found = self.peek().ttype;
        if found == TokenType::EOF {
            return false;
        }

        if found == tt {
            self.current += 1;
            return true;
        }

        false
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
    use crate::rapath::parser::{Ast, parse};
    use crate::rapath::scanner::{scan_tokens, Token, TokenType};

    struct ExprCandidate<'a> {
        e: &'a str,
        valid: bool
    }

    #[test]
    fn test_simple_expr() {
        let mut xprs = vec!();
        let x1 = ExprCandidate{e: "1+1", valid: true};
        xprs.push(x1);

        let x1 = ExprCandidate{e: "1+1 and 0 + 6", valid: true};
        xprs.push(x1);

        for x in xprs {
            let tokens = scan_tokens(&String::from(x.e)).unwrap();
            let result = parse(tokens);
            if x.valid {
                assert!(result.is_ok());
                println!("{:?}", result.unwrap());
            }
            else {
                assert!(!x.valid);
            }
        }
    }
}