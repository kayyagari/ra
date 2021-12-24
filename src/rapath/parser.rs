use crate::rapath::scanner::{Token, TokenType};
use crate::parser::ParseError;
use crate::rapath::scanner::TokenType::*;
use crate::rapath::expr::{Expr, SystemBoolean, SystemDecimal, BinaryExpr, Collection, SystemConstant, FunctionExpr};

struct Parser {
    tokens: Vec<Token>,
    current: usize,
    open_paren_count: i32,
    open_bracket_count: i32
}

pub fn parse(mut tokens: Vec<Token>) -> Result<Box<dyn Expr>, ParseError> {
    let eof = Token{ val: String::from(""), ttype: TokenType::EOF};
    tokens.push(eof);

    let mut p = Parser{ tokens, current: 0, open_paren_count: 0, open_bracket_count: 0};

    p.parse()
}

impl Parser {
    fn parse(&mut self) -> Result<Box<dyn Expr>, ParseError> {
        let e = self.expression()?;
        if e.is_none() {
            return Err(ParseError{msg: String::from("invalid expression")});
        }

        Ok(e.unwrap())
    }

    fn expression(&mut self) -> Result<Option<Box<dyn Expr>>, ParseError> {
        self.term()
    }

    fn term(&mut self) -> Result<Option<Box<dyn Expr>>, ParseError> {
        let mut left = self.literal()?;
        while self.match_tt(PLUS) || self.match_tt(MINUS) {
            let t = self.previous().ttype;
            let right = self.literal()?;
            left = Box::new(BinaryExpr{left, right, op: t});
        }

        Ok(left)
    }

    fn literal(&mut self) -> Result<Option<Box<dyn Expr>>, ParseError> {
        if self.match_tt(LEFT_BRACE) {
            self.consume(RIGHT_BRACE)?;
            let c: Collection<bool> = Collection::new();
            return Ok(Option::Some(Box::new(c)));
        }

        if self.match_tt(TRUE) {
            return Ok(Option::Some(Box::new(SystemBoolean{val: true})));
        }

        if self.match_tt(FALSE) {
            return Ok(Option::Some(Box::new(SystemBoolean{val: false})));
        }

        if self.match_tt(NUMBER) {
            // TODO separate integer and decimal
            // TODO handle quantity
            let sd = SystemDecimal::from(&self.previous().val)?;
            return Ok(Option::Some(Box::new(sd)));
        }

        Ok(Option::None)
    }

    fn constant(&mut self) -> Result<Option<Box<dyn Expr>>, ParseError> {
        if self.match_tt(CONSTANT) {
            let e = SystemConstant{val: self.previous().val.clone()};
            return Ok(Option::Some(Box::new(e)));
        }

        Ok(Option::None)
    }

    fn invocation(&mut self) -> Result<Option<Box<dyn Expr>>, ParseError> {
        let next = self.peek().ttype;
        let after_next = self.peek_double().ttype;
        match next {
            DOLLAR_TOTAL | DOLLAR_INDEX | DOLLAR_THIS => {

            }
        }
    }

    fn function(&mut self) -> Result<Option<Box<dyn Expr>>, ParseError> {
        if self.match_tt(IDENTIFIER) {
            let name = self.previous().val.clone();
            self.consume(LEFT_PAREN);
            let mut params: Vec<Box<dyn Expr>> = vec!();
            while !self.match_tt(RIGHT_PAREN) {
                let e = self.expression()?;
                params.push(e);
                if self.match_tt(COMMA) {
                    self.advance();
                }
            }
            self.consume(RIGHT_PAREN);

            let f = FunctionExpr{ name, params};
            return Ok(Option::Some(Box::new(f)));
        }

        Ok(Option::None)
    }

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
    use crate::rapath::scanner::{scan_tokens, TokenType, Token};
    use crate::rapath::parser::{BinaryExpr, SystemDecimal, Expr, parse};

    #[test]
    fn test_simple_expr() {
        let input = String::from("1+1");
        let tokens = scan_tokens(&input).unwrap();
        // let left = Box::new(SystemDecimal{val: 1 as f64});
        // let right = Box::new(SystemDecimal{val: 1 as f64});
        // let be = BinaryExpr{left , right, op: TokenType::PLUS};
        // let result = be.eval();
        // let result = parse(tokens);
        // assert!(result.is_ok());
        // let eval_result = result.unwrap().eval();
        // assert!(eval_result.is_ok());
        // let eval_result = eval_result.unwrap();
        // println!("{}", eval_result.to_string());
    }
}