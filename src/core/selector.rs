//! JMS (SQL-92 subset) message selectors, as supported by ActiveMQ Classic.

use super::message::{Message, Value};
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub struct SelectorError(pub String);

impl fmt::Display for SelectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid selector: {}", self.0)
    }
}

impl std::error::Error for SelectorError {}

#[derive(Clone, Debug)]
pub struct Selector {
    source: String,
    expression: Expr,
}

impl Selector {
    pub fn parse(source: &str) -> Result<Self, SelectorError> {
        let tokens = tokenize(source)?;
        let mut parser = Parser {
            tokens,
            position: 0,
        };
        let expression = parser.parse_or()?;
        if parser.position != parser.tokens.len() {
            return Err(SelectorError(format!(
                "unexpected token {:?}",
                parser.tokens[parser.position]
            )));
        }
        if !expression.is_boolean() {
            return Err(SelectorError(
                "selector must be a boolean expression".to_owned(),
            ));
        }
        Ok(Self {
            source: source.to_owned(),
            expression,
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn matches(&self, message: &Message) -> bool {
        matches!(self.expression.evaluate(message), Operand::Bool(true))
    }
}

/// Parses an optional selector, treating blank strings as "no selector".
pub fn parse_optional(source: Option<&str>) -> Result<Option<Selector>, SelectorError> {
    match source.map(str::trim) {
        None | Some("") => Ok(None),
        Some(source) => Selector::parse(source).map(Some),
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Identifier(String),
    StringLiteral(String),
    Integer(i64),
    Float(f64),
    Keyword(Keyword),
    Symbol(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Keyword {
    And,
    Or,
    Not,
    Between,
    Like,
    Escape,
    In,
    Is,
    Null,
    True,
    False,
}

fn tokenize(source: &str) -> Result<Vec<Token>, SelectorError> {
    let chars = source.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        let current = chars[index];
        if current.is_whitespace() {
            index += 1;
            continue;
        }
        if current == '\'' {
            let mut value = String::new();
            index += 1;
            loop {
                match chars.get(index) {
                    None => return Err(SelectorError("unterminated string literal".to_owned())),
                    Some('\'') if chars.get(index + 1) == Some(&'\'') => {
                        value.push('\'');
                        index += 2;
                    }
                    Some('\'') => {
                        index += 1;
                        break;
                    }
                    Some(character) => {
                        value.push(*character);
                        index += 1;
                    }
                }
            }
            tokens.push(Token::StringLiteral(value));
            continue;
        }
        if current.is_ascii_digit()
            || (current == '.' && chars.get(index + 1).is_some_and(char::is_ascii_digit))
        {
            let start = index;
            if current == '0'
                && matches!(chars.get(index + 1), Some('x' | 'X'))
                && chars.get(index + 2).is_some_and(char::is_ascii_hexdigit)
            {
                index += 2;
                while chars.get(index).is_some_and(char::is_ascii_hexdigit) {
                    index += 1;
                }
                let digits = chars[start + 2..index].iter().collect::<String>();
                if matches!(chars.get(index), Some('l' | 'L')) {
                    index += 1;
                }
                let value = i64::from_str_radix(&digits, 16)
                    .map_err(|_| SelectorError(format!("invalid hex literal 0x{digits}")))?;
                tokens.push(Token::Integer(value));
                continue;
            }
            let mut is_float = false;
            while let Some(character) = chars.get(index) {
                if character.is_ascii_digit() {
                    index += 1;
                } else if *character == '.' {
                    is_float = true;
                    index += 1;
                } else if matches!(character, 'e' | 'E')
                    && (chars.get(index + 1).is_some_and(char::is_ascii_digit)
                        || (matches!(chars.get(index + 1), Some('+' | '-'))
                            && chars.get(index + 2).is_some_and(char::is_ascii_digit)))
                {
                    is_float = true;
                    index += 2;
                } else {
                    break;
                }
            }
            let literal = chars[start..index].iter().collect::<String>();
            match chars.get(index) {
                Some('l' | 'L') if !is_float => index += 1,
                Some('f' | 'F' | 'd' | 'D') => {
                    is_float = true;
                    index += 1;
                }
                _ => {}
            }
            if is_float {
                tokens.push(Token::Float(literal.parse().map_err(|_| {
                    SelectorError(format!("invalid numeric literal {literal}"))
                })?));
            } else if literal.len() > 1 && literal.starts_with('0') {
                tokens.push(Token::Integer(i64::from_str_radix(&literal, 8).map_err(
                    |_| SelectorError(format!("invalid octal literal {literal}")),
                )?));
            } else {
                tokens.push(Token::Integer(literal.parse().map_err(|_| {
                    SelectorError(format!("invalid numeric literal {literal}"))
                })?));
            }
            continue;
        }
        if current.is_alphabetic() || current == '_' || current == '$' {
            let start = index;
            while chars
                .get(index)
                .is_some_and(|c| c.is_alphanumeric() || matches!(c, '_' | '$' | '.'))
            {
                index += 1;
            }
            let word = chars[start..index].iter().collect::<String>();
            let keyword = match word.to_ascii_uppercase().as_str() {
                "AND" => Some(Keyword::And),
                "OR" => Some(Keyword::Or),
                "NOT" => Some(Keyword::Not),
                "BETWEEN" => Some(Keyword::Between),
                "LIKE" => Some(Keyword::Like),
                "ESCAPE" => Some(Keyword::Escape),
                "IN" => Some(Keyword::In),
                "IS" => Some(Keyword::Is),
                "NULL" => Some(Keyword::Null),
                "TRUE" => Some(Keyword::True),
                "FALSE" => Some(Keyword::False),
                "XPATH" | "XQUERY" => {
                    return Err(SelectorError(
                        "XPath/XQuery selectors are not supported".to_owned(),
                    ));
                }
                _ => None,
            };
            tokens.push(match keyword {
                Some(keyword) => Token::Keyword(keyword),
                None => Token::Identifier(word),
            });
            continue;
        }
        let two = chars
            .get(index..index + 2)
            .map(|pair| pair.iter().collect::<String>());
        let symbol = match two.as_deref() {
            Some("<>") => Some("<>"),
            Some("!=") => Some("<>"),
            Some("<=") => Some("<="),
            Some(">=") => Some(">="),
            _ => None,
        };
        if let Some(symbol) = symbol {
            tokens.push(Token::Symbol(symbol));
            index += 2;
            continue;
        }
        let symbol = match current {
            '=' => "=",
            '<' => "<",
            '>' => ">",
            '+' => "+",
            '-' => "-",
            '*' => "*",
            '/' => "/",
            '%' => "%",
            '(' => "(",
            ')' => ")",
            ',' => ",",
            _ => return Err(SelectorError(format!("unexpected character '{current}'"))),
        };
        tokens.push(Token::Symbol(symbol));
        index += 1;
    }
    Ok(tokens)
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Arithmetic {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Clone, Debug)]
enum Expr {
    Literal(Operand),
    Identifier(String),
    Not(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Compare(Comparison, Box<Expr>, Box<Expr>),
    Arithmetic(Arithmetic, Box<Expr>, Box<Expr>),
    Negate(Box<Expr>),
    Between {
        value: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    In {
        value: Box<Expr>,
        list: Vec<String>,
        negated: bool,
    },
    Like {
        value: Box<Expr>,
        pattern: Vec<LikePart>,
        negated: bool,
    },
    IsNull {
        value: Box<Expr>,
        negated: bool,
    },
}

impl Expr {
    fn is_boolean(&self) -> bool {
        match self {
            Self::Literal(Operand::Bool(_)) | Self::Identifier(_) => true,
            Self::Literal(_) | Self::Arithmetic(..) | Self::Negate(_) => false,
            _ => true,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum LikePart {
    Literal(char),
    AnyOne,
    AnyMany,
}

#[derive(Clone, Debug, PartialEq)]
enum Operand {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.position).cloned();
        self.position += 1;
        token
    }

    fn eat_keyword(&mut self, keyword: Keyword) -> bool {
        if self.peek() == Some(&Token::Keyword(keyword)) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn eat_symbol(&mut self, symbol: &str) -> bool {
        if matches!(self.peek(), Some(Token::Symbol(found)) if *found == symbol) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn expect_symbol(&mut self, symbol: &str) -> Result<(), SelectorError> {
        if self.eat_symbol(symbol) {
            Ok(())
        } else {
            Err(SelectorError(format!("expected '{symbol}'")))
        }
    }

    fn parse_or(&mut self) -> Result<Expr, SelectorError> {
        let mut left = self.parse_and()?;
        while self.eat_keyword(Keyword::Or) {
            let right = self.parse_and()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, SelectorError> {
        let mut left = self.parse_not()?;
        while self.eat_keyword(Keyword::And) {
            let right = self.parse_not()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, SelectorError> {
        if self.eat_keyword(Keyword::Not) {
            return Ok(Expr::Not(Box::new(self.parse_not()?)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr, SelectorError> {
        let left = self.parse_additive()?;
        let comparison = match self.peek() {
            Some(Token::Symbol("=")) => Some(Comparison::Equal),
            Some(Token::Symbol("<>")) => Some(Comparison::NotEqual),
            Some(Token::Symbol("<")) => Some(Comparison::Less),
            Some(Token::Symbol("<=")) => Some(Comparison::LessEqual),
            Some(Token::Symbol(">")) => Some(Comparison::Greater),
            Some(Token::Symbol(">=")) => Some(Comparison::GreaterEqual),
            _ => None,
        };
        if let Some(comparison) = comparison {
            self.position += 1;
            let right = self.parse_additive()?;
            return Ok(Expr::Compare(comparison, Box::new(left), Box::new(right)));
        }
        if self.eat_keyword(Keyword::Is) {
            let negated = self.eat_keyword(Keyword::Not);
            if !self.eat_keyword(Keyword::Null) {
                return Err(SelectorError("expected NULL after IS".to_owned()));
            }
            return Ok(Expr::IsNull {
                value: Box::new(left),
                negated,
            });
        }
        let negated = self.eat_keyword(Keyword::Not);
        if self.eat_keyword(Keyword::Between) {
            let low = self.parse_additive()?;
            if !self.eat_keyword(Keyword::And) {
                return Err(SelectorError("expected AND in BETWEEN".to_owned()));
            }
            let high = self.parse_additive()?;
            return Ok(Expr::Between {
                value: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated,
            });
        }
        if self.eat_keyword(Keyword::Like) {
            let Some(Token::StringLiteral(pattern)) = self.next() else {
                return Err(SelectorError(
                    "LIKE requires a string literal pattern".to_owned(),
                ));
            };
            let escape = if self.eat_keyword(Keyword::Escape) {
                let Some(Token::StringLiteral(escape)) = self.next() else {
                    return Err(SelectorError("ESCAPE requires a string literal".to_owned()));
                };
                let mut chars = escape.chars();
                match (chars.next(), chars.next()) {
                    (Some(character), None) => Some(character),
                    _ => {
                        return Err(SelectorError(
                            "ESCAPE must be a single character".to_owned(),
                        ));
                    }
                }
            } else {
                None
            };
            return Ok(Expr::Like {
                value: Box::new(left),
                pattern: compile_like(&pattern, escape)?,
                negated,
            });
        }
        if self.eat_keyword(Keyword::In) {
            self.expect_symbol("(")?;
            let mut list = Vec::new();
            loop {
                match self.next() {
                    Some(Token::StringLiteral(value)) => list.push(value),
                    _ => return Err(SelectorError("IN list requires string literals".to_owned())),
                }
                if self.eat_symbol(")") {
                    break;
                }
                self.expect_symbol(",")?;
            }
            return Ok(Expr::In {
                value: Box::new(left),
                list,
                negated,
            });
        }
        if negated {
            return Err(SelectorError(
                "expected BETWEEN, LIKE or IN after NOT".to_owned(),
            ));
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr, SelectorError> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let operation = if self.eat_symbol("+") {
                Arithmetic::Add
            } else if self.eat_symbol("-") {
                Arithmetic::Subtract
            } else {
                return Ok(left);
            };
            let right = self.parse_multiplicative()?;
            left = Expr::Arithmetic(operation, Box::new(left), Box::new(right));
        }
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, SelectorError> {
        let mut left = self.parse_unary()?;
        loop {
            let operation = if self.eat_symbol("*") {
                Arithmetic::Multiply
            } else if self.eat_symbol("/") {
                Arithmetic::Divide
            } else if self.eat_symbol("%") {
                Arithmetic::Modulo
            } else {
                return Ok(left);
            };
            let right = self.parse_unary()?;
            left = Expr::Arithmetic(operation, Box::new(left), Box::new(right));
        }
    }

    fn parse_unary(&mut self) -> Result<Expr, SelectorError> {
        if self.eat_symbol("-") {
            return Ok(match self.parse_unary()? {
                Expr::Literal(Operand::Integer(value)) => {
                    Expr::Literal(Operand::Integer(value.wrapping_neg()))
                }
                Expr::Literal(Operand::Float(value)) => Expr::Literal(Operand::Float(-value)),
                other => Expr::Negate(Box::new(other)),
            });
        }
        if self.eat_symbol("+") {
            return self.parse_unary();
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, SelectorError> {
        match self.next() {
            Some(Token::Symbol("(")) => {
                let expression = self.parse_or()?;
                self.expect_symbol(")")?;
                Ok(expression)
            }
            Some(Token::Identifier(name)) => Ok(Expr::Identifier(name)),
            Some(Token::StringLiteral(value)) => Ok(Expr::Literal(Operand::String(value))),
            Some(Token::Integer(value)) => Ok(Expr::Literal(Operand::Integer(value))),
            Some(Token::Float(value)) => Ok(Expr::Literal(Operand::Float(value))),
            Some(Token::Keyword(Keyword::True)) => Ok(Expr::Literal(Operand::Bool(true))),
            Some(Token::Keyword(Keyword::False)) => Ok(Expr::Literal(Operand::Bool(false))),
            Some(Token::Keyword(Keyword::Null)) => Ok(Expr::Literal(Operand::Null)),
            Some(token) => Err(SelectorError(format!("unexpected token {token:?}"))),
            None => Err(SelectorError("unexpected end of selector".to_owned())),
        }
    }
}

fn compile_like(pattern: &str, escape: Option<char>) -> Result<Vec<LikePart>, SelectorError> {
    let mut parts = Vec::new();
    let mut chars = pattern.chars();
    while let Some(character) = chars.next() {
        if Some(character) == escape {
            match chars.next() {
                Some(escaped) => parts.push(LikePart::Literal(escaped)),
                None => return Err(SelectorError("dangling ESCAPE character".to_owned())),
            }
        } else if character == '%' {
            parts.push(LikePart::AnyMany);
        } else if character == '_' {
            parts.push(LikePart::AnyOne);
        } else {
            parts.push(LikePart::Literal(character));
        }
    }
    Ok(parts)
}

fn like_matches(pattern: &[LikePart], text: &[char]) -> bool {
    // Dynamic programming over (pattern position, text position).
    let mut current = vec![false; text.len() + 1];
    current[0] = true;
    for part in pattern {
        let mut next = vec![false; text.len() + 1];
        match part {
            LikePart::AnyMany => {
                let mut reachable = false;
                for index in 0..=text.len() {
                    reachable |= current[index];
                    next[index] = reachable;
                }
            }
            LikePart::AnyOne => {
                next[1..=text.len()].copy_from_slice(&current[..text.len()]);
            }
            LikePart::Literal(expected) => {
                for index in 0..text.len() {
                    next[index + 1] = current[index] && text[index] == *expected;
                }
            }
        }
        current = next;
    }
    current[text.len()]
}

impl Expr {
    fn evaluate(&self, message: &Message) -> Operand {
        match self {
            Self::Literal(value) => value.clone(),
            Self::Identifier(name) => resolve_identifier(name, message),
            Self::Not(inner) => match inner.evaluate(message) {
                Operand::Bool(value) => Operand::Bool(!value),
                _ => Operand::Null,
            },
            Self::And(left, right) => match (left.evaluate(message), right.evaluate(message)) {
                (Operand::Bool(false), _) | (_, Operand::Bool(false)) => Operand::Bool(false),
                (Operand::Bool(true), Operand::Bool(true)) => Operand::Bool(true),
                _ => Operand::Null,
            },
            Self::Or(left, right) => match (left.evaluate(message), right.evaluate(message)) {
                (Operand::Bool(true), _) | (_, Operand::Bool(true)) => Operand::Bool(true),
                (Operand::Bool(false), Operand::Bool(false)) => Operand::Bool(false),
                _ => Operand::Null,
            },
            Self::Compare(comparison, left, right) => compare(
                *comparison,
                &left.evaluate(message),
                &right.evaluate(message),
            ),
            Self::Arithmetic(operation, left, right) => arithmetic(
                *operation,
                &left.evaluate(message),
                &right.evaluate(message),
            ),
            Self::Negate(inner) => match inner.evaluate(message) {
                Operand::Integer(value) => Operand::Integer(value.wrapping_neg()),
                Operand::Float(value) => Operand::Float(-value),
                _ => Operand::Null,
            },
            Self::Between {
                value,
                low,
                high,
                negated,
            } => {
                let value = value.evaluate(message);
                let lower = compare(Comparison::GreaterEqual, &value, &low.evaluate(message));
                let upper = compare(Comparison::LessEqual, &value, &high.evaluate(message));
                let result = match (lower, upper) {
                    (Operand::Bool(false), _) | (_, Operand::Bool(false)) => Operand::Bool(false),
                    (Operand::Bool(true), Operand::Bool(true)) => Operand::Bool(true),
                    _ => Operand::Null,
                };
                negate_if(result, *negated)
            }
            Self::In {
                value,
                list,
                negated,
            } => match value.evaluate(message) {
                Operand::String(value) => negate_if(Operand::Bool(list.contains(&value)), *negated),
                _ => Operand::Null,
            },
            Self::Like {
                value,
                pattern,
                negated,
            } => match value.evaluate(message) {
                Operand::String(value) => negate_if(
                    Operand::Bool(like_matches(pattern, &value.chars().collect::<Vec<_>>())),
                    *negated,
                ),
                _ => Operand::Null,
            },
            Self::IsNull { value, negated } => {
                let is_null = value.evaluate(message) == Operand::Null;
                Operand::Bool(is_null != *negated)
            }
        }
    }
}

fn negate_if(value: Operand, negated: bool) -> Operand {
    match value {
        Operand::Bool(value) => Operand::Bool(value != negated),
        other => other,
    }
}

fn resolve_identifier(name: &str, message: &Message) -> Operand {
    match name {
        "JMSDeliveryMode" => {
            return Operand::String(
                if message.persistent {
                    "PERSISTENT"
                } else {
                    "NON_PERSISTENT"
                }
                .to_owned(),
            );
        }
        "JMSPriority" => return Operand::Integer(i64::from(message.priority)),
        "JMSMessageID" => return Operand::String(message.message_id.clone()),
        "JMSTimestamp" => return Operand::Integer(message.timestamp),
        "JMSExpiration" => return Operand::Integer(message.expiration),
        "JMSRedelivered" => return Operand::Bool(message.redelivery_counter > 0),
        "JMSCorrelationID" => {
            return message
                .correlation_id
                .clone()
                .map_or(Operand::Null, Operand::String);
        }
        "JMSType" => {
            return message
                .message_type
                .clone()
                .map_or(Operand::Null, Operand::String);
        }
        "JMSDestination" => return Operand::String(message.destination.to_uri()),
        "JMSReplyTo" => {
            return message
                .reply_to
                .as_ref()
                .map_or(Operand::Null, |reply| Operand::String(reply.to_uri()));
        }
        "JMSXGroupID" => {
            return message
                .group_id
                .clone()
                .map_or(Operand::Null, Operand::String);
        }
        "JMSXGroupSeq" => return Operand::Integer(i64::from(message.group_sequence)),
        "JMSXDeliveryCount" => return Operand::Integer(i64::from(message.redelivery_counter) + 1),
        "JMSXUserID" => {
            return message
                .user_id
                .clone()
                .map_or(Operand::Null, Operand::String);
        }
        _ => {}
    }
    match message.properties.get(name) {
        None | Some(Value::Null) => Operand::Null,
        Some(Value::Bool(value)) => Operand::Bool(*value),
        Some(Value::Byte(value)) => Operand::Integer(i64::from(*value)),
        Some(Value::Short(value)) => Operand::Integer(i64::from(*value)),
        Some(Value::Int(value)) => Operand::Integer(i64::from(*value)),
        Some(Value::Long(value)) => Operand::Integer(*value),
        Some(Value::Float(value)) => Operand::Float(f64::from(*value)),
        Some(Value::Double(value)) => Operand::Float(*value),
        Some(Value::String(value)) => Operand::String(value.clone()),
        Some(Value::Char(value)) => char::from_u32(u32::from(*value))
            .map_or(Operand::Null, |character| {
                Operand::String(character.to_string())
            }),
        Some(_) => Operand::Null,
    }
}

fn numeric_pair(left: &Operand, right: &Operand) -> Option<(f64, f64, bool)> {
    match (left, right) {
        (Operand::Integer(left), Operand::Integer(right)) => {
            Some((*left as f64, *right as f64, true))
        }
        (Operand::Integer(left), Operand::Float(right)) => Some((*left as f64, *right, false)),
        (Operand::Float(left), Operand::Integer(right)) => Some((*left, *right as f64, false)),
        (Operand::Float(left), Operand::Float(right)) => Some((*left, *right, false)),
        _ => None,
    }
}

fn compare(comparison: Comparison, left: &Operand, right: &Operand) -> Operand {
    if let (Operand::Integer(left), Operand::Integer(right)) = (left, right) {
        return Operand::Bool(apply_ordering(comparison, left.cmp(right)));
    }
    if let Some((left, right, _)) = numeric_pair(left, right) {
        return match left.partial_cmp(&right) {
            Some(ordering) => Operand::Bool(apply_ordering(comparison, ordering)),
            None => Operand::Bool(false),
        };
    }
    match (left, right) {
        (Operand::String(left), Operand::String(right)) => match comparison {
            Comparison::Equal => Operand::Bool(left == right),
            Comparison::NotEqual => Operand::Bool(left != right),
            _ => Operand::Null,
        },
        (Operand::Bool(left), Operand::Bool(right)) => match comparison {
            Comparison::Equal => Operand::Bool(left == right),
            Comparison::NotEqual => Operand::Bool(left != right),
            _ => Operand::Null,
        },
        (Operand::Null, _) | (_, Operand::Null) => Operand::Null,
        _ => match comparison {
            // Comparing incompatible types yields false (ActiveMQ behaviour).
            Comparison::Equal => Operand::Bool(false),
            Comparison::NotEqual => Operand::Bool(true),
            _ => Operand::Null,
        },
    }
}

fn apply_ordering(comparison: Comparison, ordering: std::cmp::Ordering) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    match comparison {
        Comparison::Equal => ordering == Equal,
        Comparison::NotEqual => ordering != Equal,
        Comparison::Less => ordering == Less,
        Comparison::LessEqual => ordering != Greater,
        Comparison::Greater => ordering == Greater,
        Comparison::GreaterEqual => ordering != Less,
    }
}

fn arithmetic(operation: Arithmetic, left: &Operand, right: &Operand) -> Operand {
    if let (Operand::Integer(left), Operand::Integer(right)) = (left, right) {
        return match operation {
            Arithmetic::Add => Operand::Integer(left.wrapping_add(*right)),
            Arithmetic::Subtract => Operand::Integer(left.wrapping_sub(*right)),
            Arithmetic::Multiply => Operand::Integer(left.wrapping_mul(*right)),
            Arithmetic::Divide if *right == 0 => Operand::Null,
            Arithmetic::Divide => Operand::Integer(left.wrapping_div(*right)),
            Arithmetic::Modulo if *right == 0 => Operand::Null,
            Arithmetic::Modulo => Operand::Integer(left.wrapping_rem(*right)),
        };
    }
    match numeric_pair(left, right) {
        Some((left, right, _)) => Operand::Float(match operation {
            Arithmetic::Add => left + right,
            Arithmetic::Subtract => left - right,
            Arithmetic::Multiply => left * right,
            Arithmetic::Divide => left / right,
            Arithmetic::Modulo => left % right,
        }),
        None => Operand::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::destination::Destination;

    fn message() -> Message {
        Message::text(Destination::queue("orders"), "hello")
            .with_property("color", "red")
            .with_property("weight", 12)
            .with_property("price", Value::Double(9.5))
            .with_property("rush", true)
    }

    fn check(source: &str) -> bool {
        Selector::parse(source).unwrap().matches(&message())
    }

    #[test]
    fn evaluates_comparisons_and_logic() {
        assert!(check("color = 'red'"));
        assert!(!check("color <> 'red'"));
        assert!(check("weight > 10 AND price < 10.0"));
        assert!(check("weight * 2 = 24"));
        assert!(check("NOT (weight < 5) OR color = 'blue'"));
        assert!(check("rush"));
        assert!(check("rush = TRUE"));
        assert!(check("JMSPriority = 4 AND JMSDeliveryMode = 'PERSISTENT'"));
        assert!(check("weight + 0.5 = 12.5"));
        assert!(check("weight = 0xC"));
    }

    #[test]
    fn evaluates_between_in_like_and_null() {
        assert!(check("weight BETWEEN 10 AND 20"));
        assert!(check("weight NOT BETWEEN 13 AND 20"));
        assert!(check("color IN ('green', 'red')"));
        assert!(check("color NOT IN ('green', 'blue')"));
        assert!(check("color LIKE 'r_d'"));
        assert!(check("color LIKE '%e%'"));
        assert!(!check("color LIKE 'b%'"));
        assert!(check("missing IS NULL"));
        assert!(check("color IS NOT NULL"));
        assert!(
            Selector::parse("x LIKE '10\\%' ESCAPE '\\'")
                .unwrap()
                .matches(&message().with_property("x", "10%"))
        );
    }

    #[test]
    fn unknown_values_do_not_match() {
        assert!(!check("missing = 'x'"));
        assert!(!check("NOT (missing = 'x')"));
        assert!(check("missing = 'x' OR color = 'red'"));
        assert!(!check("color > 5"));
    }

    #[test]
    fn rejects_invalid_selectors() {
        assert!(Selector::parse("color = ").is_err());
        assert!(Selector::parse("weight + 1").is_err());
        assert!(Selector::parse("'unterminated").is_err());
        assert!(Selector::parse("XPATH '//a'").is_err());
    }
}
