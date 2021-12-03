use crate::scanner::*;
use crate::scanner::TokenType::*;
use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::parser::ExprType::*;

struct Parser {
    tokens: Vec<Token>,
    current: usize
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum ExprType {
    SIMPLE,
    CONDITIONAL,
    AND,
    NOT,
    OR
}

pub trait Expr {
    fn get_type(&self) -> ExprType;
    fn to_string(&self) -> String;
}

#[derive(Debug)]
pub struct ParseError {
    msg: String
}

#[derive(Debug)]
pub struct SimpleExpr {
    identifier: String,
    operator: &'static ComparisonOperator,
    value: String
}

pub struct CondExpr {
    identifier: String,
    id_path: String,
    operator: &'static ComparisonOperator,
    value: String,
    condition: Box<dyn Expr>
}

pub struct AndExpr {
    children: Vec<Box<dyn Expr>>
}

pub struct OrExpr {
    children: Vec<Box<dyn Expr>>
}

pub struct NotExpr {
    child: Box<dyn Expr>
}

impl Expr for CondExpr{
    fn get_type(&self) -> ExprType {
        CONDITIONAL
    }

    fn to_string(&self) -> String {
        format!("({}[{}]{} {:?} {})", &self.identifier, self.condition.to_string(), &self.id_path, self.operator, &self.value)
    }
}

impl Expr for SimpleExpr{
    fn get_type(&self) -> ExprType {
        SIMPLE
    }

    fn to_string(&self) -> String {
        format!("({} {:?} {})", &self.identifier, self.operator, &self.value)
    }
}

impl Expr for AndExpr{
    fn get_type(&self) -> ExprType {
        AND
    }

    fn to_string(&self) -> String {
        let mut s = String::from("(");
        let size = self.children.len() - 1;
        for (i, ch) in self.children.iter().enumerate() {
            s.push_str(ch.to_string().as_str());
            if size > 0 && i < size {
                s.push_str(" AND ");
            }
        }
        s.push_str(")");
        s
    }
}

impl Expr for OrExpr{
    fn get_type(&self) -> ExprType {
        OR
    }

    fn to_string(&self) -> String {
        let mut s = String::from("(");
        let size = self.children.len() - 1;
        for (i, ch) in self.children.iter().enumerate() {
            s.push_str(ch.to_string().as_str());
            if size > 1 && i < size {
                s.push_str(" OR ");
            }
        }
        s.push_str(")");
        s
    }
}

impl Expr for NotExpr{
    fn get_type(&self) -> ExprType {
        NOT
    }

    fn to_string(&self) -> String {
        format!("NOT{}", self.child.to_string())
    }
}

impl Error for ParseError{}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.msg.as_str())
    }
}

pub fn parse(mut tokens: Vec<Token>) -> Result<Box<dyn Expr>, ParseError> {
    let eof = Token{ val: String::from(""), ttype: TokenType::EOF};
    tokens.push(eof);

    let mut p = Parser{ tokens, current: 0};

    p.parse()
}

impl Parser {
    fn parse(&mut self) -> Result<Box<dyn Expr>, ParseError> {
        let mut e: Option<Box<dyn Expr>> = Option::None;
        while !self.is_at_end() {
            let t = self.peek();
            match t.ttype {
                IDENTIFIER => {
                    e = Option::Some(self.parse_expr()?);
                },
                LEFT_PAREN => {
                    self.advance();
                    e = Option::Some(self.parse()?);
                    self.consume(TokenType::RIGHT_PAREN)?;
                },
                LEFT_BRACKET => {
                    self.advance();
                    e = Option::Some(self.parse()?);
                    self.consume(TokenType::RIGHT_BRACKET)?;
                },
                LOGIC_OPERATOR => {
                    let op = t.val.as_str();
                    match op {
                        "and" => {
                            let mut and = Box::new(AndExpr{ children: vec![e.unwrap()] });
                            self.advance();
                            let rhs = self.parse()?;
                            and.children.push(rhs);
                            e = Option::Some(and);
                        },
                        "not" => {
                            self.advance();
                            let rhs = self.parse()?;
                            let not = Box::new(NotExpr{child: rhs});
                            e = Option::Some(not);
                        },
                        "or" => {
                            let mut or = Box::new(OrExpr{children: vec!(e.unwrap())});
                            self.advance();
                            let rhs = self.parse()?;
                            or.children.push(rhs);
                            e = Option::Some(or);
                        },
                        s => {
                            return Err(ParseError{msg: format!("invalid filter expression, found {}", s)});
                        }
                    }
                },
                _ => {
                    //return Err(ParseError{msg: format!("invalid token, type {}", t)});
                    break;
                }
            }

        }

        Ok(e.unwrap())
    }

    fn parse_expr(&mut self) -> Result<Box<dyn Expr>, ParseError> {
        let id = self.consume(IDENTIFIER)?;
        let id = id.val.clone();

        let mut cond_expr: Option<Box<dyn Expr>> = Option::None;
        let mut id_path: Option<String> = Option::None;
        if self.peek().ttype == TokenType::LEFT_BRACKET { // there is a conditional expression
            self.consume(TokenType::LEFT_BRACKET)?;
            let ce = self.parse()?;
            self.consume(TokenType::RIGHT_BRACKET)?;
            let id_path_token = self.consume(TokenType::IDENTIFIER_PATH)?;
            id_path = Option::Some(id_path_token.val.clone());
            cond_expr = Some(ce);
        }

        let op = self.consume(COMPARISON_OPERATOR)?;
        let op = OPERATORS.get(op.val.as_str()).unwrap();
        let va = self.consume(LITERAL)?;

        if cond_expr.is_some() {
            let ce = CondExpr{identifier: id, id_path: id_path.unwrap(), operator: op, value: va.val.clone(), condition: cond_expr.unwrap()};
            return Ok(Box::new(ce));
        }

        let se = SimpleExpr{identifier: id, operator: op, value: va.val.clone()};
        Ok(Box::new(se))
    }

    fn consume(&mut self, tt: TokenType) -> Result<&Token, ParseError> {
        if self.check(tt) {
            return Ok(self.advance());
        }
        let found = self.peek();
        Err(ParseError{msg: format!("expected token {} but found {}", tt, found.ttype)})
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.current]
    }

    fn is_at_end(&mut self) -> bool {
        self.peek().ttype == TokenType::EOF
    }

    fn check(&mut self, tt: TokenType) -> bool {
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

    fn previous(&mut self) -> &Token {
        &self.tokens[self.current - 1]
    }
}

#[cfg(test)]
mod tests {
    use crate::scanner::scan_tokens;
    use crate::parser::{parse, Expr, ParseError};

    fn parse_filter(filter: &String) -> Result<Box<dyn Expr>, ParseError> {
        let tokens = scan_tokens(&filter).expect("failed to scan the filter");
        parse(tokens)
    }

    struct FilterCandidate {
        input: String,
        output: String,
        success: bool
    }

    #[test]
    fn test_parse_simple() {
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

        for f in filters {
            let expr = parse_filter(&f.input);
            assert!(f.success);
            let expr = expr.unwrap();
            assert_eq!(f.output, expr.to_string());
        }
    }
}