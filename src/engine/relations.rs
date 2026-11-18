//! A view compiled against a relation the caller supplies, and the raw columns a query
//! reads.
//!
//! The decision engine simulates rows that look like a view's rows (one per candidate ×
//! scenario × entity × step) and holds them in an in-memory DuckDB table. To evaluate a
//! measure such as `ads.spend` over them, rather than restate it by hand, it derives an
//! engine whose `ads` view reads that table ([`SemanticEngine::with_relations`]), and it
//! checks up front that the simulation produces every column the measure reads
//! ([`SemanticEngine::columns_read`]).
//!
//! The derived engine is a separate value. The engine it came from is not changed, so
//! warehouse queries compile exactly as before.

use std::collections::{BTreeMap, BTreeSet};

use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::member_sql::dotted_ref_regex;
use super::query::{QueryFilter, QueryRequest};
use super::{DatasourceDialectMap, EngineError, SemanticEngine};
use crate::dialect::Dialect;
use crate::schema::models::{Dimension, View};

/// One view read from another relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationOverride {
    /// The table the view reads instead of its own `table` / `sql`.
    pub table: String,
    /// Plain columns of that table to add as dimensions of the view, for this engine
    /// only (for example `candidate`, `scenario`). Each dimension's expression is the
    /// column of the same name.
    pub dimensions: Vec<String>,
}

/// Views read from other relations, all in one dialect.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationOverrides {
    /// The dialect the relations are queried in, whatever the views' datasources say.
    pub dialect: Dialect,
    /// View name -> where it reads from.
    pub views: BTreeMap<String, RelationOverride>,
}

impl SemanticEngine {
    /// An engine whose overridden views read the given relations, in the given dialect.
    ///
    /// Only the overridden views exist in it: a query that reaches any other view (a
    /// dimension, a filter, or a join a measure needs) fails to compile, naming that view,
    /// rather than mixing the caller's rows with the warehouse's. Cross-view
    /// declarations that would need a dropped view (drivers, a `default_cohort`, entity
    /// cohorts, a derived lifespan) are left out of the derived engine; they describe
    /// analysis over the warehouse, not how a measure is computed.
    ///
    /// A member of an overridden view whose expression reaches a view that is not kept (a
    /// `{{other.x}}` reference, directly or through another such member) is left out too:
    /// it cannot be computed from the caller's rows, and the rest of the view still can.
    ///
    /// Refused: an override naming a view the layer does not declare, and a synthetic
    /// dimension whose name the view already uses.
    pub fn with_relations(
        &self,
        overrides: &RelationOverrides,
    ) -> Result<SemanticEngine, EngineError> {
        let layer = self.semantic_layer();
        for name in overrides.views.keys() {
            if layer.view_by_name(name).is_none() {
                return Err(EngineError::QueryError(format!(
                    "relation override names view '{name}', which the semantic layer does not declare"
                )));
            }
        }
        let kept = |view: &str| overrides.views.contains_key(view);
        let mut derived = layer.clone();
        derived.views.retain(|v| kept(&v.name));
        derived.topics = None;
        derived.saved_queries = None;
        for view in &mut derived.views {
            override_view(view, &overrides.views[&view.name], &kept)?;
        }
        prune_unreachable(&mut derived.views);
        SemanticEngine::from_semantic_layer(
            derived,
            DatasourceDialectMap::with_default(overrides.dialect.clone()),
        )
        .map_err(|e| {
            EngineError::QueryError(format!(
                "the overridden views do not compile on their own: {e}"
            ))
        })
    }

    /// Every raw column `request` reads, per view: the columns behind its measures
    /// (following `{{view.member}}` references into the members they name, and including
    /// each measure's `filters:`), its dimensions, time dimensions, segments and filters.
    ///
    /// Read from the expressions' tokens: a word is a column unless it is a function name
    /// (followed by `(`), a SQL keyword that shapes an expression (`CASE`, `AND`, `NULL`,
    /// ...), the type in a cast, or a table qualifier. A string literal reads nothing.
    pub fn columns_read(
        &self,
        request: &QueryRequest,
    ) -> Result<BTreeMap<String, BTreeSet<String>>, EngineError> {
        let mut out = Reader {
            engine: self,
            cols: BTreeMap::new(),
            seen: BTreeSet::new(),
        };
        let members = (request.measures.iter())
            .chain(&request.dimensions)
            .chain(&request.segments)
            .chain(request.time_dimensions.iter().map(|t| &t.dimension));
        for m in members {
            out.member(m)?;
        }
        for f in &request.filters {
            out.filter(f)?;
        }
        Ok(out.cols)
    }
}

fn override_view(
    view: &mut View,
    o: &RelationOverride,
    kept: &dyn Fn(&str) -> bool,
) -> Result<(), EngineError> {
    view.table = Some(o.table.clone());
    view.sql = None;
    view.datasource = None;
    view.dialect = None;
    view.pre_aggregations = None;
    view.refresh_key = None;
    let in_scope = |member: &str| member.split('.').next().is_some_and(kept);
    for entity in &mut view.entities {
        entity.cohorts = None;
        if (entity.lifespan.as_ref()).is_some_and(|l| l.from.as_deref().is_some_and(|f| !kept(f)))
        {
            entity.lifespan = None;
        }
    }
    for m in view.measures.iter_mut().flatten() {
        if let Some(drivers) = &mut m.drivers {
            drivers.retain(|d| in_scope(&d.measure));
        }
        // Entity cohorts are dropped above, so no `default_cohort` can resolve.
        m.default_cohort = None;
    }
    for name in &o.dimensions {
        let taken = view.dimensions.iter().any(|d| &d.name == name)
            || view.measures.iter().flatten().any(|m| &m.name == name);
        if taken {
            return Err(EngineError::QueryError(format!(
                "relation override adds dimension '{name}' to view '{}', which already declares a member of that name",
                view.name
            )));
        }
        view.dimensions.push(synthetic_dimension(name)?);
    }
    Ok(())
}

/// Drop every member whose expressions reach a view outside `views`, until none does.
fn prune_unreachable(views: &mut [View]) {
    // `{{owner.field}}`: a kept view, or the primary entity of one.
    let mut owner_view: BTreeMap<String, String> = BTreeMap::new();
    for v in views.iter() {
        owner_view.insert(v.name.clone(), v.name.clone());
        for e in &v.entities {
            if e.entity_type == crate::schema::models::EntityType::Primary {
                owner_view.insert(e.name.clone(), v.name.clone());
            }
        }
    }
    let mut dropped: BTreeSet<(String, String)> = BTreeSet::new();
    loop {
        let before = dropped.len();
        for v in views.iter_mut() {
            let reaches = |exprs: &[&str], dropped: &BTreeSet<(String, String)>| {
                exprs.iter().all(|e| {
                    dotted_ref_regex().captures_iter(e).all(|c| {
                        let (owner, field) = (&c[1], &c[2]);
                        owner == "variables"
                            || (owner_view.get(owner))
                                .is_some_and(|ov| !dropped.contains(&(ov.clone(), field.to_string())))
                    })
                })
            };
            let name = v.name.clone();
            let mut gone = vec![];
            v.dimensions.retain(|d| keep(reaches(&[&d.expr], &dropped), &d.name, &mut gone));
            v.segments.retain(|s| keep(reaches(&[&s.expr], &dropped), &s.name, &mut gone));
            for ms in v.measures.iter_mut() {
                ms.retain(|m| {
                    let mut exprs: Vec<&str> = m.expr.iter().map(String::as_str).collect();
                    exprs.extend(m.filters.iter().flatten().map(|f| f.expr.as_str()));
                    keep(reaches(&exprs, &dropped), &m.name, &mut gone)
                });
            }
            dropped.extend(gone.into_iter().map(|g| (name.clone(), g)));
        }
        if dropped.len() == before {
            break;
        }
    }
    for m in views.iter_mut().flat_map(|v| v.measures.iter_mut().flatten()) {
        if let Some(drivers) = &mut m.drivers {
            drivers.retain(|d| {
                d.measure
                    .split_once('.')
                    .is_none_or(|(v, f)| !dropped.contains(&(v.to_string(), f.to_string())))
            });
        }
    }
}

fn keep(reaches: bool, name: &str, gone: &mut Vec<String>) -> bool {
    if !reaches {
        gone.push(name.to_string());
    }
    reaches
}

fn synthetic_dimension(column: &str) -> Result<Dimension, EngineError> {
    serde_json::from_value(serde_json::json!({
        "name": column,
        "type": "number",
        "expr": column,
        "description": "Added by a relation override: a plain column of the overriding relation.",
    }))
    .map_err(|e| EngineError::QueryError(format!("dimension '{column}': {e}")))
}

struct Reader<'a> {
    engine: &'a SemanticEngine,
    cols: BTreeMap<String, BTreeSet<String>>,
    /// Members already expanded, so a reference cycle or a repeat stops.
    seen: BTreeSet<String>,
}

impl Reader<'_> {
    fn filter(&mut self, f: &QueryFilter) -> Result<(), EngineError> {
        if let Some(m) = &f.member {
            self.member(m)?;
        }
        for g in f.and.iter().chain(&f.or).flatten() {
            self.filter(g)?;
        }
        Ok(())
    }

    /// The columns behind `view.member`; a name the view does not declare as a member is
    /// itself a column of that view.
    fn member(&mut self, path: &str) -> Result<(), EngineError> {
        let (view_name, name) = path.split_once('.').ok_or_else(|| {
            EngineError::QueryError(format!("'{path}' is not a view.member reference"))
        })?;
        if !self.seen.insert(path.to_string()) {
            return Ok(());
        }
        let layer = self.engine.semantic_layer();
        let view = layer.view_by_name(view_name).ok_or_else(|| {
            EngineError::QueryError(format!("'{path}' names view '{view_name}', which is not declared"))
        })?;
        let mut exprs: Vec<String> = vec![];
        if let Some(m) = view.measures.iter().flatten().find(|m| m.name == name) {
            exprs.extend(m.expr.clone());
            exprs.extend(m.filters.iter().flatten().map(|f| f.expr.clone()));
            if let Some(shift) = &m.shift {
                self.member(&qualify(view_name, &shift.measure))?;
            }
        } else if let Some(d) = view.dimensions.iter().find(|d| d.name == name) {
            exprs.push(d.expr.clone());
        } else if let Some(s) = view.segments.iter().find(|s| s.name == name) {
            exprs.push(s.expr.clone());
        } else {
            self.column(view_name, name);
        }
        for e in exprs {
            self.expr(view_name, &e)?;
        }
        Ok(())
    }

    fn column(&mut self, view: &str, column: &str) {
        (self.cols.entry(view.to_string()).or_default()).insert(column.to_string());
    }

    /// The columns `expr` (written on `view`) reads.
    fn expr(&mut self, view: &str, expr: &str) -> Result<(), EngineError> {
        let mut refs = vec![];
        let stripped = dotted_ref_regex().replace_all(expr, |c: &regex::Captures<'_>| {
            refs.push((c[1].to_string(), c[2].to_string()));
            " 0 ".to_string()
        });
        let stripped = table_ref().replace_all(&stripped, "").to_string();
        for (owner, field) in refs {
            if owner == "variables" {
                continue;
            }
            let owner_view = self.view_for(&owner);
            self.member(&format!("{owner_view}.{field}"))?;
        }
        let tokens = Tokenizer::new(&GenericDialect {}, &stripped)
            .tokenize()
            .map_err(|e| EngineError::QueryError(format!("cannot read `{expr}`: {e}")))?;
        let tokens: Vec<&Token> = tokens
            .iter()
            .filter(|t| !matches!(t, Token::Whitespace(_)))
            .collect();
        for (i, t) in tokens.iter().enumerate() {
            let Token::Word(w) = t else { continue };
            let next = tokens.get(i + 1);
            let prev = i.checked_sub(1).and_then(|p| tokens.get(p));
            let is_call = matches!(next, Some(Token::LParen));
            let is_qualifier = matches!(next, Some(Token::Period));
            let is_type = matches!(prev, Some(Token::DoubleColon))
                || matches!(prev, Some(Token::Word(p)) if p.keyword == Keyword::AS);
            let is_keyword = w.quote_style.is_none() && SHAPE_KEYWORDS.contains(&w.keyword);
            if !(is_call || is_qualifier || is_type || is_keyword) {
                self.column(view, &w.value);
            }
        }
        Ok(())
    }

    /// The view a `{{x.field}}` reference reads: `x` itself when it is a view, else the
    /// view whose primary entity is `x`.
    fn view_for(&self, owner: &str) -> String {
        let layer = self.engine.semantic_layer();
        if layer.view_by_name(owner).is_some() {
            return owner.to_string();
        }
        (layer.views.iter())
            .find(|v| {
                v.entities.iter().any(|e| {
                    e.name == owner && e.entity_type == crate::schema::models::EntityType::Primary
                })
            })
            .map(|v| v.name.clone())
            .unwrap_or_else(|| owner.to_string())
    }
}

fn qualify(view: &str, member: &str) -> String {
    match member.contains('.') {
        true => member.to_string(),
        false => format!("{view}.{member}"),
    }
}

fn table_ref() -> &'static regex::Regex {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"\{\{\s*TABLE\s*\}\}\s*\.").unwrap())
}

/// Keywords that shape an expression rather than name a column. A keyword outside this
/// list (`day`, `date`, `value`, ...) is a legitimate column name.
const SHAPE_KEYWORDS: &[Keyword] = &[
    Keyword::CASE,
    Keyword::WHEN,
    Keyword::THEN,
    Keyword::ELSE,
    Keyword::END,
    Keyword::AND,
    Keyword::OR,
    Keyword::NOT,
    Keyword::NULL,
    Keyword::IS,
    Keyword::IN,
    Keyword::LIKE,
    Keyword::ILIKE,
    Keyword::BETWEEN,
    Keyword::AS,
    Keyword::DISTINCT,
    Keyword::TRUE,
    Keyword::FALSE,
    Keyword::INTERVAL,
    Keyword::EXISTS,
    Keyword::ALL,
    Keyword::ANY,
    Keyword::SOME,
    Keyword::FILTER,
    Keyword::OVER,
    Keyword::PARTITION,
    Keyword::BY,
    Keyword::ORDER,
    Keyword::ASC,
    Keyword::DESC,
    Keyword::NULLS,
    Keyword::FIRST,
    Keyword::LAST,
    Keyword::WITHIN,
    Keyword::GROUP,
    Keyword::SELECT,
    Keyword::FROM,
    Keyword::WHERE,
];
