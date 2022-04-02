use crate::barn::Barn;
use crate::res_schema::SchemaDef;
use crate::ResourceDef;
use crate::search::Filter;
use crate::barn::index_scanners::IndexScanner;
use crate::errors::EvalError;
use crate::search::ComparisonOperator;

pub fn execute(filter: &Filter, rd: &ResourceDef, db: &Barn, sd: &SchemaDef) {
    let idx_filter = to_index_scanner(filter, rd, sd, db);
}

pub fn to_index_scanner<'f, 'd: 'f>(filter: &'f Filter, rd: &ResourceDef, sd: &SchemaDef, db: &'d Barn) -> Result<impl IndexScanner + 'f, EvalError> {
    match filter {
        Filter::StringFilter {identifier, value,  operator} => {
            let sp_expr = sd.get_search_param_expr_for_res(identifier, &rd.name);
            if let None = sp_expr {
                return Err(EvalError::new(format!("there is no search parameter defined with code {} on {}", identifier, rd.name)));
            }
            let sp_expr = sp_expr.unwrap();
            let str_idx_scanner = db.new_string_index_scanner(&sp_expr.hash, value.as_str().as_bytes(), operator);
            return Ok(str_idx_scanner);
        },
        _ => {
        }
    }

    Err(EvalError::new(format!("unsupported filter type {:?}", filter.get_type())))
}
