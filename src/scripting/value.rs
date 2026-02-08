#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptValue {
    Number(i64),
    String(String),
    Ident(String),
    Tuple(Vec<ScriptValue>),
    List(Vec<ScriptValue>),
}

pub fn parse_value(raw: &str) -> Result<ScriptValue, String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("empty value".to_string());
    }
    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        return Ok(ScriptValue::String(
            value[1..value.len() - 1].to_string(),
        ));
    }
    if value.starts_with('{') && value.ends_with('}') {
        let inner = value[1..value.len() - 1].trim();
        if inner.is_empty() {
            return Ok(ScriptValue::List(Vec::new()));
        }
        let parts = split_top_level(inner, ',')?;
        let mut items = Vec::with_capacity(parts.len());
        for part in parts {
            items.push(parse_value(&part)?);
        }
        return Ok(ScriptValue::List(items));
    }
    if value.starts_with('[') && value.ends_with(']') {
        let inner = value[1..value.len() - 1].trim();
        if inner.is_empty() {
            return Ok(ScriptValue::List(Vec::new()));
        }
        let parts = split_top_level(inner, ',')?;
        let mut items = Vec::with_capacity(parts.len());
        for part in parts {
            items.push(parse_value(&part)?);
        }
        return Ok(ScriptValue::List(items));
    }
    if value.starts_with('(') && value.ends_with(')') {
        let inner = value[1..value.len() - 1].trim();
        if inner.is_empty() {
            return Ok(ScriptValue::Tuple(Vec::new()));
        }
        let parts = split_top_level(inner, ',')?;
        let mut items = Vec::with_capacity(parts.len());
        for part in parts {
            items.push(parse_value(&part)?);
        }
        return Ok(ScriptValue::Tuple(items));
    }
    if let Ok(number) = value.parse::<i64>() {
        return Ok(ScriptValue::Number(number));
    }
    Ok(ScriptValue::Ident(value.to_string()))
}

pub fn split_top_level(input: &str, delimiter: char) -> Result<Vec<String>, String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_quotes = false;
    let mut prev_escape = false;

    for ch in input.chars() {
        if in_quotes {
            if ch == '"' && !prev_escape {
                in_quotes = false;
            }
            prev_escape = ch == '\\' && !prev_escape;
            current.push(ch);
            continue;
        }

        match ch {
            '"' => {
                in_quotes = true;
                prev_escape = false;
                current.push(ch);
            }
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                if paren_depth == 0 {
                    return Err("unbalanced ')'".to_string());
                }
                paren_depth -= 1;
                current.push(ch);
            }
            '{' => {
                brace_depth += 1;
                current.push(ch);
            }
            '}' => {
                if brace_depth == 0 {
                    return Err("unbalanced '}'".to_string());
                }
                brace_depth -= 1;
                current.push(ch);
            }
            '[' => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' => {
                if bracket_depth == 0 {
                    return Err("unbalanced ']'".to_string());
                }
                bracket_depth -= 1;
                current.push(ch);
            }
            _ if ch == delimiter
                && paren_depth == 0
                && brace_depth == 0
                && bracket_depth == 0 =>
            {
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if in_quotes {
        return Err("unterminated string".to_string());
    }
    if paren_depth != 0 || brace_depth != 0 || bracket_depth != 0 {
        return Err("unbalanced delimiters".to_string());
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_value_accepts_bracket_list() {
        let parsed = parse_value("[1,2,3]").expect("parse list");
        assert_eq!(
            parsed,
            ScriptValue::List(vec![
                ScriptValue::Number(1),
                ScriptValue::Number(2),
                ScriptValue::Number(3)
            ])
        );
    }
}
