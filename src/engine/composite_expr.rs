//! The parsed form of a composite measure's expression.
//!
//! A `type: number` / `type: custom` measure is a function of other measures:
//! `{{v.a}} / NULLIF({{v.b}}, 0)`, `if({{v.h}} > 0, 100 * {{v.c}} / {{v.s}}, NULL)`.
//! Reading that function off the text one `{{ref}}` at a time — the operator
//! next to it, whether it sits in a condition — cannot describe what the
//! measure does when an input moves: a repeat is a second factor in `a * a`
//! and nothing in a guard, a ref in `a / (b + c)` is a denominator although
//! a `+` is next to it. So the expression is parsed once into a syntax tree
//! and asked directly: what are your inputs, what are you at these values,
//! and are you linear in your inputs.
//!
//! `{{view.measure}}` refs (and `{{variables.X}}`, which match the same
//! pattern) are swapped for placeholder identifiers before parsing, so the
//! SQL parser sees plain names.
//!
//! Evaluation is deliberately strict. Anything whose value cannot be known
//! from the measures' values alone — a bare column, a raw `SUM(x)`, a function
//! not listed here, a `{{variables.X}}` — is an error naming the construct, and
//! a construct whose NULL or rounding semantics differ across warehouses is
//! refused rather than guessed. A caller turns an error into a reported
//! refusal; it never gets a number that only looks right.

use sqlparser::ast::{
    BinaryOperator, CeilFloorKind, DataType, DateTimeField, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, ObjectNamePart, UnaryOperator, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

const PLACEHOLDER: &str = "__airlayer_ref_";

/// A composite measure's expression, parsed.
#[derive(Debug, Clone)]
pub struct CompositeExpr {
    ast: Expr,
    /// Placeholder index -> ref id (`view.measure`), distinct, in first-read order.
    refs: Vec<String>,
}

/// Why an expression could not be evaluated.
#[derive(Debug, Clone, PartialEq)]
pub enum EvalError {
    /// A ref the expression reads has no value. Usually fixed by supplying one.
    MissingValue(String),
    /// The expression does something evaluation cannot reproduce faithfully.
    Unsupported(String),
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalError::MissingValue(r) => write!(f, "no value for `{r}`"),
            EvalError::Unsupported(why) => f.write_str(why),
        }
    }
}

impl CompositeExpr {
    /// Parse `expr`. The error is the parser's own message.
    pub fn parse(expr: &str) -> Result<Self, String> {
        let mut refs: Vec<String> = Vec::new();
        let sql = crate::engine::member_sql::dotted_ref_regex().replace_all(
            expr,
            |cap: &regex::Captures<'_>| {
                let id = format!("{}.{}", &cap[1], &cap[2]);
                let i = match refs.iter().position(|r| *r == id) {
                    Some(i) => i,
                    None => {
                        refs.push(id);
                        refs.len() - 1
                    }
                };
                format!(" {PLACEHOLDER}{i} ")
            },
        );
        let mut parser = Parser::new(&GenericDialect {})
            .try_with_sql(&sql)
            .map_err(|e| e.to_string())?;
        let ast = parser.parse_expr().map_err(|e| e.to_string())?;
        // `parse_expr` stops at the first token that cannot continue the
        // expression; anything left over means the text was not one expression.
        let rest = parser.peek_token();
        if rest.token != sqlparser::tokenizer::Token::EOF {
            return Err(format!("unexpected `{}` after the expression", rest.token));
        }
        Ok(CompositeExpr { ast, refs })
    }

    /// The distinct refs the expression reads, in first-read order.
    pub fn refs(&self) -> &[String] {
        &self.refs
    }

    /// The expression's value with each ref read through `value_of`.
    /// `Ok(None)` is SQL NULL.
    pub fn eval(&self, value_of: &dyn Fn(&str) -> Option<f64>) -> Result<Option<f64>, EvalError> {
        self.eval_in(&Cx {
            value_of,
            int_div: false,
        })
    }

    /// [`eval`](Self::eval) as a warehouse that integer-divides would compute
    /// it: a `/` between two whole numbers, neither side floating-point by its
    /// own text, truncates toward zero — Postgres, Redshift, Presto and SQLite
    /// on two integer aggregates. Whether the warehouse does is not knowable
    /// from the expression; comparing both readings with a fetched level is.
    pub fn eval_integer_division(
        &self,
        value_of: &dyn Fn(&str) -> Option<f64>,
    ) -> Result<Option<f64>, EvalError> {
        self.eval_in(&Cx {
            value_of,
            int_div: true,
        })
    }

    fn eval_in(&self, cx: &Cx<'_>) -> Result<Option<f64>, EvalError> {
        match self.ev(&self.ast, cx)? {
            V::Num(n) => Ok(Some(n)),
            V::Null => Ok(None),
            other => Err(unsupported(format!(
                "evaluates to {}, not a number",
                other.kind()
            ))),
        }
    }

    /// `Some(coefficients)` when the expression is affine in its refs —
    /// `c0 + Σ cᵢ·refᵢ` — so a move in any ref moves it by `cᵢ·Δ` wherever
    /// the inputs sit. `None` for anything else, including any conditional.
    pub fn linear_coefficients(&self) -> Option<HashMap<String, f64>> {
        self.lin(&self.ast).map(|(_, coefs)| coefs)
    }

    /// The ref id behind a placeholder identifier, if it is one.
    fn ref_of(&self, ident: &str) -> Option<&str> {
        let i: usize = ident.strip_prefix(PLACEHOLDER)?.parse().ok()?;
        self.refs.get(i).map(String::as_str)
    }

    fn ident(&self, name: &str, cx: &Cx<'_>) -> Result<V, EvalError> {
        let Some(id) = self.ref_of(name) else {
            return Err(unsupported(format!(
                "reads `{name}`, a column rather than a measure, so it has no value here"
            )));
        };
        if id.starts_with("variables.") {
            return Err(unsupported(format!(
                "reads `{{{{{id}}}}}`, a variable whose value is not known here"
            )));
        }
        (cx.value_of)(id)
            .map(V::Num)
            .ok_or_else(|| EvalError::MissingValue(id.to_string()))
    }

    fn ev(&self, e: &Expr, cx: &Cx<'_>) -> Result<V, EvalError> {
        let ev = |x: &Expr| self.ev(x, cx);
        match e {
            Expr::Identifier(id) => self.ident(&id.value, cx),
            Expr::Value(v) => match &v.value {
                Value::Number(n, _) => n
                    .parse::<f64>()
                    .map(V::Num)
                    .map_err(|_| unsupported(format!("reads the number `{n}`, which is not one"))),
                Value::SingleQuotedString(s) => Ok(V::Str(s.clone())),
                Value::Boolean(b) => Ok(V::Bool(*b)),
                Value::Null => Ok(V::Null),
                other => Err(unsupported(format!("uses the literal `{other}`"))),
            },
            Expr::Nested(x) => ev(x),
            Expr::UnaryOp { op, expr } => match (op, ev(expr)?) {
                (_, V::Null) => Ok(V::Null),
                (UnaryOperator::Minus, V::Num(n)) => Ok(V::Num(-n)),
                (UnaryOperator::Plus, V::Num(n)) => Ok(V::Num(n)),
                (UnaryOperator::Not, V::Bool(b)) => Ok(V::Bool(!b)),
                (op, v) => Err(unsupported(format!("applies `{op}` to {}", v.kind()))),
            },
            Expr::BinaryOp { left, op, right } => self.binary(left, op, right, cx),
            Expr::IsNull(x) => Ok(V::Bool(ev(x)? == V::Null)),
            Expr::IsNotNull(x) => Ok(V::Bool(ev(x)? != V::Null)),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let (x, lo, hi) = (ev(expr)?, ev(low)?, ev(high)?);
                let inside = and3(
                    compare(&x, &lo)?.map(|o| o.is_ge()),
                    compare(&x, &hi)?.map(|o| o.is_le()),
                );
                Ok(bool3(inside.map(|b| b != *negated)))
            }
            Expr::Cast {
                expr, data_type, ..
            } => {
                if !is_float_type(data_type) {
                    return Err(unsupported(format!(
                        "casts to `{data_type}`; only a floating-point cast leaves a value \
                         alone on every warehouse (an integer cast rounds on some and \
                         truncates on others, and a bare DECIMAL is scale 0 on several)"
                    )));
                }
                match ev(expr)? {
                    v @ (V::Num(_) | V::Null) => Ok(v),
                    v => Err(unsupported(format!("casts {} to a number", v.kind()))),
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let subject = operand.as_deref().map(ev).transpose()?;
                for when in conditions {
                    let hit = match &subject {
                        // Simple CASE: `CASE x WHEN v THEN …` is `x = v`.
                        Some(x) => compare(x, &ev(&when.condition)?)?.map(|o| o.is_eq()),
                        None => truth(&ev(&when.condition)?)?,
                    };
                    if hit == Some(true) {
                        return ev(&when.result);
                    }
                }
                else_result.as_deref().map_or(Ok(V::Null), ev)
            }
            Expr::Function(f) => self.call(f, cx),
            // sqlparser reads the `FLOOR`/`CEIL` keywords into their own nodes;
            // only the plain one-argument form is a number function.
            Expr::Floor {
                expr,
                field: CeilFloorKind::DateTimeField(DateTimeField::NoDateTime),
            } => match ev(expr)? {
                V::Num(n) => finite(n.floor()),
                V::Null => Ok(V::Null),
                v => Err(unsupported(format!("passes {} to `FLOOR()`", v.kind()))),
            },
            Expr::Ceil {
                expr,
                field: CeilFloorKind::DateTimeField(DateTimeField::NoDateTime),
            } => match ev(expr)? {
                V::Num(n) => finite(n.ceil()),
                V::Null => Ok(V::Null),
                v => Err(unsupported(format!("passes {} to `CEIL()`", v.kind()))),
            },
            other => Err(unsupported(format!("uses `{other}`"))),
        }
    }

    fn binary(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        cx: &Cx<'_>,
    ) -> Result<V, EvalError> {
        let ev = |x: &Expr| self.ev(x, cx);
        // AND/OR stop at a decided left side, as a guard like
        // `h > 0 AND c / h >= 13` is written to.
        match op {
            BinaryOperator::And => {
                let l = truth(&ev(left)?)?;
                if l == Some(false) {
                    return Ok(V::Bool(false));
                }
                return Ok(bool3(and3(l, truth(&ev(right)?)?)));
            }
            BinaryOperator::Or => {
                let l = truth(&ev(left)?)?;
                if l == Some(true) {
                    return Ok(V::Bool(true));
                }
                let r = truth(&ev(right)?)?;
                return Ok(bool3(match (l, r) {
                    (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                }));
            }
            _ => {}
        }
        let (l, r) = (ev(left)?, ev(right)?);
        let ordering = |pick: fn(std::cmp::Ordering) -> bool| -> Result<V, EvalError> {
            Ok(bool3(compare(&l, &r)?.map(pick)))
        };
        match op {
            BinaryOperator::Gt => return ordering(|o| o.is_gt()),
            BinaryOperator::Lt => return ordering(|o| o.is_lt()),
            BinaryOperator::GtEq => return ordering(|o| o.is_ge()),
            BinaryOperator::LtEq => return ordering(|o| o.is_le()),
            BinaryOperator::Eq => return ordering(|o| o.is_eq()),
            BinaryOperator::NotEq => return ordering(|o| o.is_ne()),
            _ => {}
        }
        let (a, b) = match (l, r) {
            (V::Null, _) | (_, V::Null) => return Ok(V::Null),
            (V::Num(a), V::Num(b)) => (a, b),
            (l, r) => {
                return Err(unsupported(format!(
                    "applies `{op}` to {} and {}",
                    l.kind(),
                    r.kind()
                )))
            }
        };
        let n = match op {
            BinaryOperator::Plus => a + b,
            BinaryOperator::Minus => a - b,
            BinaryOperator::Multiply => a * b,
            BinaryOperator::Divide if b == 0.0 => {
                return Err(unsupported(
                    "divides by zero at these values, which errors on some warehouses \
                     and is NULL on others — guard the divisor with NULLIF"
                        .to_string(),
                ))
            }
            BinaryOperator::Divide
                if cx.int_div
                    && a.fract() == 0.0
                    && b.fract() == 0.0
                    && !known_float(left)
                    && !known_float(right) =>
            {
                (a / b).trunc()
            }
            BinaryOperator::Divide => a / b,
            other => return Err(unsupported(format!("uses the `{other}` operator"))),
        };
        finite(n)
    }

    fn call(&self, f: &Function, cx: &Cx<'_>) -> Result<V, EvalError> {
        let shown = f.name.to_string();
        let refuse = || {
            unsupported(format!(
                "calls `{shown}()`, which cannot be evaluated from measure values"
            ))
        };
        if f.over.is_some() || f.filter.is_some() || !f.within_group.is_empty() {
            return Err(refuse());
        }
        let name = match f.name.0.last() {
            Some(ObjectNamePart::Identifier(id)) => id.value.to_ascii_uppercase(),
            _ => return Err(refuse()),
        };
        let FunctionArguments::List(list) = &f.args else {
            return Err(refuse());
        };
        if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
            return Err(refuse());
        }
        let args: Vec<&Expr> = list
            .args
            .iter()
            .map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
                _ => Err(refuse()),
            })
            .collect::<Result<_, _>>()?;
        let ev = |x: &Expr| self.ev(x, cx);
        let arity = |ok: &[usize]| -> Result<(), EvalError> {
            if ok.contains(&args.len()) {
                Ok(())
            } else {
                Err(unsupported(format!(
                    "calls `{shown}()` with {} arguments",
                    args.len()
                )))
            }
        };
        let num = |x: &Expr| -> Result<Option<f64>, EvalError> {
            match ev(x)? {
                V::Num(n) => Ok(Some(n)),
                V::Null => Ok(None),
                v => Err(unsupported(format!("passes {} to `{shown}()`", v.kind()))),
            }
        };
        // A one-argument numeric function: NULL in, NULL out.
        let unary = |g: fn(f64) -> f64| -> Result<V, EvalError> {
            arity(&[1])?;
            num(args[0])?.map_or(Ok(V::Null), |x| finite(g(x)))
        };

        match name.as_str() {
            // Lazy: only the selected branch is evaluated, as SQL does, so
            // `if(b > 0, a / b, 0)` is not a division by zero at b = 0.
            "IF" | "IFF" | "IIF" => {
                arity(&[2, 3])?;
                if truth(&ev(args[0])?)? == Some(true) {
                    ev(args[1])
                } else {
                    args.get(2).map_or(Ok(V::Null), |e| ev(e))
                }
            }
            "COALESCE" | "IFNULL" | "NVL" => {
                if name != "COALESCE" {
                    arity(&[2])?;
                }
                for a in &args {
                    let v = ev(a)?;
                    if v != V::Null {
                        return Ok(v);
                    }
                }
                Ok(V::Null)
            }
            "NULLIF" => {
                arity(&[2])?;
                let (a, b) = (ev(args[0])?, ev(args[1])?);
                Ok(if compare(&a, &b)? == Some(std::cmp::Ordering::Equal) {
                    V::Null
                } else {
                    a
                })
            }
            "ZEROIFNULL" => {
                arity(&[1])?;
                Ok(V::Num(num(args[0])?.unwrap_or(0.0)))
            }
            "NULLIFZERO" => {
                arity(&[1])?;
                Ok(num(args[0])?.filter(|x| *x != 0.0).map_or(V::Null, V::Num))
            }
            // A NULL dividend gives NULL even over a zero divisor here. If a
            // warehouse returns 0 there instead, the cost is a refusal (an
            // undefined level), never a wrong number.
            "SAFE_DIVIDE" | "DIV0" | "DIV0NULL" => {
                arity(&[2])?;
                let (a, b) = (num(args[0])?, num(args[1])?);
                match (name.as_str(), a, b) {
                    ("DIV0NULL", Some(_), None) | ("DIV0NULL", None, None) => Ok(V::Num(0.0)),
                    (_, None, _) | (_, _, None) => Ok(V::Null),
                    ("SAFE_DIVIDE", _, Some(0.0)) => Ok(V::Null),
                    (_, _, Some(0.0)) => Ok(V::Num(0.0)),
                    (_, Some(n), Some(d)) => finite(n / d),
                }
            }
            "GREATEST" | "LEAST" => {
                if args.is_empty() {
                    return Err(refuse());
                }
                let mut best: Option<f64> = None;
                for a in &args {
                    let Some(x) = num(a)? else {
                        return Err(unsupported(format!(
                            "passes NULL to `{shown}()`, which some warehouses skip and \
                             others propagate"
                        )));
                    };
                    best = Some(match best {
                        None => x,
                        Some(b) if name == "GREATEST" => b.max(x),
                        Some(b) => b.min(x),
                    });
                }
                Ok(best.map_or(V::Null, V::Num))
            }
            "ABS" => unary(f64::abs),
            "SIGN" => unary(|x| if x == 0.0 { 0.0 } else { x.signum() }),
            "SQRT" => unary(f64::sqrt),
            "EXP" => unary(f64::exp),
            "LN" => unary(f64::ln),
            "LOG10" => unary(f64::log10),
            "CEILING" => unary(f64::ceil),
            "ROUND" => Err(unsupported(
                "calls `ROUND()`, which breaks ties to even on some warehouses and away \
                 from zero on others"
                    .to_string(),
            )),
            "POWER" | "POW" => {
                arity(&[2])?;
                match (num(args[0])?, num(args[1])?) {
                    (Some(a), Some(b)) => finite(a.powf(b)),
                    _ => Ok(V::Null),
                }
            }
            _ => Err(refuse()),
        }
    }

    /// `(constant, coefficients)` when `e` is affine in the refs.
    fn lin(&self, e: &Expr) -> Option<(f64, HashMap<String, f64>)> {
        let scale = |(c, m): (f64, HashMap<String, f64>), k: f64| {
            (c * k, m.into_iter().map(|(r, v)| (r, v * k)).collect())
        };
        match e {
            Expr::Identifier(id) => {
                let r = self.ref_of(&id.value)?;
                (!r.starts_with("variables.")).then(|| (0.0, HashMap::from([(r.to_string(), 1.0)])))
            }
            Expr::Value(v) => match &v.value {
                Value::Number(n, _) => n.parse().ok().map(|n| (n, HashMap::new())),
                _ => None,
            },
            Expr::Nested(x) => self.lin(x),
            Expr::Cast {
                expr, data_type, ..
            } if is_float_type(data_type) => self.lin(expr),
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => Some(scale(self.lin(expr)?, -1.0)),
            Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr,
            } => self.lin(expr),
            Expr::BinaryOp { left, op, right } => {
                let (l, r) = (self.lin(left)?, self.lin(right)?);
                match op {
                    BinaryOperator::Plus | BinaryOperator::Minus => {
                        let k = if *op == BinaryOperator::Plus {
                            1.0
                        } else {
                            -1.0
                        };
                        let (rc, rm) = scale(r, k);
                        let (c, mut m) = l;
                        for (id, v) in rm {
                            *m.entry(id).or_insert(0.0) += v;
                        }
                        Some((c + rc, m))
                    }
                    BinaryOperator::Multiply if l.1.is_empty() => Some(scale(r, l.0)),
                    BinaryOperator::Multiply if r.1.is_empty() => Some(scale(l, r.0)),
                    // Only a division that is floating-point on every warehouse:
                    // `{{a}} / 4` integer-divides an integer aggregate on
                    // Postgres, Redshift, Presto and SQLite.
                    BinaryOperator::Divide
                        if r.1.is_empty()
                            && r.0 != 0.0
                            && (known_float(left) || known_float(right)) =>
                    {
                        Some(scale(l, 1.0 / r.0))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

/// How refs are read, and which division a warehouse is assumed to do.
struct Cx<'a> {
    value_of: &'a dyn Fn(&str) -> Option<f64>,
    int_div: bool,
}

/// An evaluated SQL value.
#[derive(Debug, Clone, PartialEq)]
enum V {
    Num(f64),
    Bool(bool),
    Str(String),
    Null,
}

impl V {
    fn kind(&self) -> &'static str {
        match self {
            V::Num(_) => "a number",
            V::Bool(_) => "a boolean",
            V::Str(_) => "a string",
            V::Null => "NULL",
        }
    }
}

fn unsupported(why: String) -> EvalError {
    EvalError::Unsupported(why)
}

fn finite(n: f64) -> Result<V, EvalError> {
    if n.is_finite() {
        Ok(V::Num(n))
    } else {
        Err(unsupported(
            "produces a non-finite number at these values".to_string(),
        ))
    }
}

/// A condition's truth: `Some(bool)`, or `None` for NULL (unknown).
fn truth(v: &V) -> Result<Option<bool>, EvalError> {
    match v {
        V::Bool(b) => Ok(Some(*b)),
        V::Null => Ok(None),
        other => Err(unsupported(format!(
            "uses {} where a condition is expected",
            other.kind()
        ))),
    }
}

fn bool3(b: Option<bool>) -> V {
    b.map_or(V::Null, V::Bool)
}

fn and3(l: Option<bool>, r: Option<bool>) -> Option<bool> {
    match (l, r) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

/// SQL comparison: `None` when either side is NULL.
fn compare(l: &V, r: &V) -> Result<Option<std::cmp::Ordering>, EvalError> {
    match (l, r) {
        (V::Null, _) | (_, V::Null) => Ok(None),
        (V::Num(a), V::Num(b)) => Ok(a.partial_cmp(b)),
        (V::Str(a), V::Str(b)) => Ok(Some(a.cmp(b))),
        (V::Bool(a), V::Bool(b)) => Ok(Some(a.cmp(b))),
        (l, r) => Err(unsupported(format!(
            "compares {} with {}",
            l.kind(),
            r.kind()
        ))),
    }
}

/// A cast that leaves a number's value alone on every warehouse. Decimal types
/// are not: a bare `DECIMAL`/`NUMERIC` is scale 0 on Snowflake, MySQL and
/// Databricks, and a declared scale rounds.
fn is_float_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Float(_)
            | DataType::Double(_)
            | DataType::DoublePrecision
            | DataType::Real
            | DataType::Float4
            | DataType::Float8
            | DataType::Float32
            | DataType::Float64
    )
}

/// Whether `e` is floating-point by its own text — a float literal or a float
/// cast, through arithmetic — so a `/` against it is float division on every
/// warehouse, whatever the measures' column types.
fn known_float(e: &Expr) -> bool {
    match e {
        Expr::Value(v) => matches!(&v.value, Value::Number(n, _) if n.contains(['.', 'e', 'E'])),
        Expr::Cast { data_type, .. } => is_float_type(data_type),
        Expr::Nested(x) | Expr::UnaryOp { expr: x, .. } => known_float(x),
        Expr::BinaryOp { left, right, .. } => known_float(left) || known_float(right),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    fn eval(expr: &str, pairs: &[(&str, f64)]) -> Result<Option<f64>, EvalError> {
        let vals = values(pairs);
        CompositeExpr::parse(expr)
            .unwrap_or_else(|e| panic!("{expr} should parse: {e}"))
            .eval(&|r| vals.get(r).copied())
    }

    fn unsupported(expr: &str, pairs: &[(&str, f64)]) -> String {
        match eval(expr, pairs) {
            Err(EvalError::Unsupported(why)) => why,
            other => panic!("{expr} should be refused, got {other:?}"),
        }
    }

    const WAGE_COST: &str = "if({{ v.hours }} > 0 AND {{ v.cost }} / {{ v.hours }} >= 13.0, \
                             100.0 * {{ v.cost }} / NULLIF({{ v.sales }}, 0), NULL)";

    #[test]
    fn refs_are_distinct_in_first_read_order() {
        let e = CompositeExpr::parse(WAGE_COST).unwrap();
        assert_eq!(e.refs(), ["v.hours", "v.cost", "v.sales"]);
    }

    #[test]
    fn evaluates_arithmetic() {
        assert_eq!(eval("{{v.a}} * 12", &[("v.a", 10.0)]), Ok(Some(120.0)));
        assert_eq!(
            eval(
                "({{v.a}} - {{v.b}}) / -2 + 1",
                &[("v.a", 10.0), ("v.b", 4.0)]
            ),
            Ok(Some(-2.0))
        );
    }

    #[test]
    fn a_guard_selects_the_value_and_a_failed_guard_is_null() {
        let base = [
            ("v.hours", 1_000.0),
            ("v.cost", 27_090.0),
            ("v.sales", 100_000.0),
        ];
        let got = eval(WAGE_COST, &base).unwrap().unwrap();
        assert!((got - 27.09).abs() < 1e-9, "got {got}");

        // $12/hour fails the guard: the measure is undefined, not zero.
        let cheap = [
            ("v.hours", 1_000.0),
            ("v.cost", 12_000.0),
            ("v.sales", 100_000.0),
        ];
        assert_eq!(eval(WAGE_COST, &cheap), Ok(None));
    }

    #[test]
    fn case_when_and_its_aliases_follow_sql_semantics() {
        let e = "CASE WHEN {{v.a}} > 10 THEN 1 WHEN {{v.a}} > 5 THEN 2 ELSE 3 END";
        assert_eq!(eval(e, &[("v.a", 20.0)]), Ok(Some(1.0)));
        assert_eq!(eval(e, &[("v.a", 7.0)]), Ok(Some(2.0)));
        assert_eq!(eval(e, &[("v.a", 1.0)]), Ok(Some(3.0)));
        // No ELSE: NULL.
        assert_eq!(
            eval("CASE WHEN {{v.a}} > 1 THEN 1 END", &[("v.a", 0.0)]),
            Ok(None)
        );
        // Simple CASE.
        assert_eq!(
            eval("CASE {{v.a}} WHEN 2 THEN 9 END", &[("v.a", 2.0)]),
            Ok(Some(9.0))
        );
        assert_eq!(
            eval("IFF({{v.a}} > 0, 1, 2)", &[("v.a", -1.0)]),
            Ok(Some(2.0))
        );
        assert_eq!(
            eval("iif({{v.a}} > 0, 1, 2)", &[("v.a", 1.0)]),
            Ok(Some(1.0))
        );
    }

    #[test]
    fn null_propagates_and_three_valued_logic_holds() {
        assert_eq!(
            eval(
                "{{v.a}} / NULLIF({{v.b}}, 0)",
                &[("v.a", 1.0), ("v.b", 0.0)]
            ),
            Ok(None)
        );
        assert_eq!(
            eval("COALESCE(NULLIF({{v.b}}, 0), 7)", &[("v.b", 0.0)]),
            Ok(Some(7.0))
        );
        // NULL AND FALSE is FALSE, so the guard fails to the else branch.
        assert_eq!(
            eval("if(NULL AND {{v.a}} > 1, 1, 2)", &[("v.a", 0.0)]),
            Ok(Some(2.0))
        );
        assert_eq!(
            eval("if({{v.a}} IS NULL, 1, 2)", &[("v.a", 0.0)]),
            Ok(Some(2.0))
        );
    }

    #[test]
    fn numeric_casts_pass_through() {
        assert_eq!(
            eval("CAST({{v.a}} AS DOUBLE) / 4", &[("v.a", 2.0)]),
            Ok(Some(0.5))
        );
        assert_eq!(eval("{{v.a}}::float / 4", &[("v.a", 2.0)]), Ok(Some(0.5)));
    }

    #[test]
    fn a_ref_without_a_value_is_named() {
        assert_eq!(
            eval("{{v.a}} / {{v.b}}", &[("v.a", 1.0)]),
            Err(EvalError::MissingValue("v.b".to_string()))
        );
    }

    #[test]
    fn what_cannot_be_evaluated_faithfully_is_refused_by_name() {
        assert!(unsupported("{{v.a}} / 0", &[("v.a", 1.0)]).contains("zero"));
        assert!(unsupported("SUM(amount) / {{v.a}}", &[("v.a", 1.0)]).contains("SUM"));
        assert!(unsupported("{{v.a}} * amount", &[("v.a", 1.0)]).contains("amount"));
        assert!(unsupported("FROBNICATE({{v.a}})", &[("v.a", 1.0)]).contains("FROBNICATE"));
        // Integer casts round on some warehouses and truncate on others.
        assert!(unsupported("CAST({{v.a}} AS INTEGER)", &[("v.a", 1.5)]).contains("INT"));
        // A variable's value is not a measure value predict can know.
        assert!(unsupported("{{v.a}} * {{variables.fx}}", &[("v.a", 1.0)]).contains("variables.fx"));
    }

    #[test]
    fn a_decimal_cast_and_round_are_refused_for_their_rounding() {
        // A bare DECIMAL is scale 0 on Snowflake, MySQL and Databricks.
        assert!(unsupported("CAST({{v.a}} AS DECIMAL) / 3", &[("v.a", 10.4)]).contains("DECIMAL"));
        assert_eq!(
            CompositeExpr::parse("CAST({{v.a}} AS DECIMAL) / 3.0")
                .unwrap()
                .linear_coefficients(),
            None
        );
        // Ties round to even on some warehouses and away from zero on others.
        assert!(unsupported("ROUND({{v.a}}, 1)", &[("v.a", 2.25)]).contains("ROUND"));
    }

    #[test]
    fn floor_and_ceil_evaluate() {
        assert_eq!(eval("FLOOR({{v.a}})", &[("v.a", 2.7)]), Ok(Some(2.0)));
        assert_eq!(eval("CEIL({{v.a}})", &[("v.a", 2.1)]), Ok(Some(3.0)));
        assert_eq!(eval("CEILING({{v.a}})", &[("v.a", -2.1)]), Ok(Some(-2.0)));
    }

    #[test]
    fn an_unparseable_expression_is_an_error() {
        assert!(CompositeExpr::parse("{{v.a}} * * 2").is_err());
    }

    #[test]
    fn linear_coefficients_cover_affine_expressions_only() {
        let lin = |e: &str| CompositeExpr::parse(e).unwrap().linear_coefficients();
        assert_eq!(
            lin("{{v.a}} + {{v.b}} - {{v.c}}"),
            Some(values(&[("v.a", 1.0), ("v.b", 1.0), ("v.c", -1.0)]))
        );
        assert_eq!(lin("{{v.a}} * 12"), Some(values(&[("v.a", 12.0)])));
        assert_eq!(
            lin("({{v.a}} - {{v.b}}) / 4.0 + 3"),
            Some(values(&[("v.a", 0.25), ("v.b", -0.25)]))
        );
        assert_eq!(
            lin("CAST({{v.a}} AS DOUBLE) / 4"),
            Some(values(&[("v.a", 0.25)]))
        );
        assert_eq!(lin("{{v.a}} * 1.0 / 4"), Some(values(&[("v.a", 0.25)])));
        // Integer division on Postgres/Redshift/Presto/SQLite when `a` is an
        // integer aggregate: not linear, so not sized without its level.
        assert_eq!(lin("{{v.a}} / 4"), None);
        assert_eq!(lin("{{v.a}} + {{v.a}}"), Some(values(&[("v.a", 2.0)])));
        assert_eq!(
            lin("CAST({{v.a}} AS DOUBLE) * 2"),
            Some(values(&[("v.a", 2.0)]))
        );
        assert_eq!(lin("{{v.a}} * {{v.a}}"), None);
        assert_eq!(lin("{{v.a}} / {{v.b}}"), None);
        assert_eq!(lin("{{v.a}} / 0"), None);
        assert_eq!(lin("if({{v.a}} > 0, {{v.a}}, 0)"), None);
        assert_eq!(lin("{{v.a}} * {{variables.fx}}"), None);
    }
}
