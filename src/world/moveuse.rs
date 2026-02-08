use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseDatabase {
    pub sections: Vec<MoveUseSection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseSection {
    pub name: String,
    pub rules: Vec<MoveUseRule>,
    pub children: Vec<MoveUseSection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseRule {
    pub event: MoveUseExpr,
    pub conditions: Vec<MoveUseExpr>,
    pub actions: Vec<MoveUseExpr>,
    pub line_no: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseExpr {
    pub name: String,
    pub args: Vec<String>,
}

pub fn load_moveuse(path: &Path) -> Result<MoveUseDatabase, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|err| format!("failed to read moveuse.dat {}: {}", path.display(), err))?;
    parse_moveuse(&content)
}

fn parse_moveuse(content: &str) -> Result<MoveUseDatabase, String> {
    let mut sections = Vec::new();
    let mut stack: Vec<usize> = Vec::new();

    for (index, raw_line) in content.lines().enumerate() {
        let line_no = index + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(name) = parse_begin(line, line_no)? {
            let section = MoveUseSection {
                name,
                rules: Vec::new(),
                children: Vec::new(),
            };
            if stack.is_empty() {
                sections.push(section);
                stack.push(sections.len() - 1);
            } else {
                let parent = section_mut(&mut sections, &stack)
                    .ok_or_else(|| format!("moveuse.dat invalid section stack at line {}", line_no))?;
                parent.children.push(section);
                stack.push(parent.children.len() - 1);
            }
            continue;
        }

        if line == "END" {
            if stack.pop().is_none() {
                return Err(format!("moveuse.dat unexpected END at line {}", line_no));
            }
            continue;
        }

        let rule = parse_rule(line, line_no)?;
        let current = section_mut(&mut sections, &stack)
            .ok_or_else(|| format!("moveuse.dat rule outside section at line {}", line_no))?;
        current.rules.push(rule);
    }

    if !stack.is_empty() {
        return Err("moveuse.dat missing END for section".to_string());
    }

    Ok(MoveUseDatabase { sections })
}

fn parse_begin(line: &str, line_no: usize) -> Result<Option<String>, String> {
    if !line.starts_with("BEGIN") {
        return Ok(None);
    }
    let start = line.find('"').ok_or_else(|| {
        format!("moveuse.dat BEGIN missing '\"' at line {}", line_no)
    })?;
    let end = line.rfind('"').ok_or_else(|| {
        format!("moveuse.dat BEGIN missing closing '\"' at line {}", line_no)
    })?;
    if end <= start {
        return Err(format!(
            "moveuse.dat BEGIN invalid quote order at line {}",
            line_no
        ));
    }
    Ok(Some(line[start + 1..end].to_string()))
}

fn parse_rule(line: &str, line_no: usize) -> Result<MoveUseRule, String> {
    let line = strip_trailing_comment(line);
    let (lhs, rhs) = split_arrow(&line).ok_or_else(|| {
        format!("moveuse.dat rule missing -> at line {}", line_no)
    })?;
    let conditions = split_top_level(lhs, ',');
    if conditions.is_empty() {
        return Err(format!(
            "moveuse.dat rule missing event at line {}",
            line_no
        ));
    }
    let mut lhs_iter = conditions.into_iter();
    let event = parse_expr(lhs_iter.next().unwrap(), line_no)?;
    let mut condition_exprs = Vec::new();
    for condition in lhs_iter {
        condition_exprs.push(parse_expr(condition, line_no)?);
    }

    let mut action_exprs = Vec::new();
    for action in split_top_level(rhs, ',') {
        action_exprs.push(parse_expr(action, line_no)?);
    }
    if action_exprs.is_empty() {
        return Err(format!(
            "moveuse.dat rule missing actions at line {}",
            line_no
        ));
    }

    Ok(MoveUseRule {
        event,
        conditions: condition_exprs,
        actions: action_exprs,
        line_no,
    })
}

fn strip_trailing_comment(line: &str) -> String {
    let end = line.trim_end().len();
    if end == 0 {
        return String::new();
    }

    let bytes = line.as_bytes();

    // Find a top-level trailing quoted comment at the end of the line.
    let mut i = end;
    while i > 0 && line.as_bytes()[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    if i == 0 || line.as_bytes()[i - 1] != b'"' {
        return line.trim_end().to_string();
    }

    let mut in_quotes = false;
    let mut paren_depth = 0i32;
    let mut bracket_depth = 0i32;
    let mut quote_start = None;
    let mut idx = 0usize;
    while idx < i {
        let ch = bytes[idx] as char;
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                if in_quotes && paren_depth == 0 && bracket_depth == 0 {
                    quote_start = Some(idx);
                }
            }
            '(' if !in_quotes => paren_depth += 1,
            ')' if !in_quotes && paren_depth > 0 => paren_depth -= 1,
            '[' if !in_quotes => bracket_depth += 1,
            ']' if !in_quotes && bracket_depth > 0 => bracket_depth -= 1,
            _ => {}
        }
        idx += 1;
    }

    if let Some(start) = quote_start {
        if start < i && paren_depth == 0 && bracket_depth == 0 {
            let trimmed = line[..start].trim_end();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }

    line.trim_end().to_string()
}

fn parse_expr(raw: &str, line_no: usize) -> Result<MoveUseExpr, String> {
    let token = raw.trim();
    if token.is_empty() {
        return Err(format!(
            "moveuse.dat empty expression at line {}",
            line_no
        ));
    }
    if let Some(open) = token.find('(') {
        let close = token.rfind(')').ok_or_else(|| {
            format!("moveuse.dat missing ')' at line {}", line_no)
        })?;
        if close <= open {
            return Err(format!(
                "moveuse.dat invalid parentheses at line {}",
                line_no
            ));
        }
        let name = token[..open].trim();
        if name.is_empty() {
            return Err(format!(
                "moveuse.dat missing expression name at line {}",
                line_no
            ));
        }
        let args_raw = &token[open + 1..close];
        let mut args = Vec::new();
        for arg in split_top_level(args_raw, ',') {
            let arg = arg.trim();
            if !arg.is_empty() {
                args.push(arg.to_string());
            }
        }
        Ok(MoveUseExpr {
            name: name.to_string(),
            args,
        })
    } else {
        Ok(MoveUseExpr {
            name: token.to_string(),
            args: Vec::new(),
        })
    }
}

fn split_arrow(input: &str) -> Option<(&str, &str)> {
    let mut in_quotes = false;
    let mut paren_depth = 0i32;
    let mut bracket_depth = 0i32;
    let bytes = input.as_bytes();
    let mut idx = 0usize;
    while idx + 1 < bytes.len() {
        let ch = bytes[idx] as char;
        match ch {
            '"' => in_quotes = !in_quotes,
            '(' if !in_quotes => paren_depth += 1,
            ')' if !in_quotes && paren_depth > 0 => paren_depth -= 1,
            '[' if !in_quotes => bracket_depth += 1,
            ']' if !in_quotes && bracket_depth > 0 => bracket_depth -= 1,
            '-' if !in_quotes && paren_depth == 0 && bracket_depth == 0 => {
                if bytes[idx + 1] as char == '>' {
                    let left = input[..idx].trim();
                    let right = input[idx + 2..].trim();
                    return Some((left, right));
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

fn split_top_level(input: &str, delimiter: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut in_quotes = false;
    let mut paren_depth = 0i32;
    let mut bracket_depth = 0i32;
    let mut start = 0usize;

    for (index, ch) in input.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            '(' if !in_quotes => paren_depth += 1,
            ')' if !in_quotes && paren_depth > 0 => paren_depth -= 1,
            '[' if !in_quotes => bracket_depth += 1,
            ']' if !in_quotes && bracket_depth > 0 => bracket_depth -= 1,
            _ => {}
        }

        if ch == delimiter && !in_quotes && paren_depth == 0 && bracket_depth == 0 {
            parts.push(input[start..index].trim());
            start = index + 1;
        }
    }

    if start <= input.len() {
        parts.push(input[start..].trim());
    }

    parts.into_iter().filter(|part| !part.is_empty()).collect()
}

fn section_mut<'a>(
    sections: &'a mut Vec<MoveUseSection>,
    path: &[usize],
) -> Option<&'a mut MoveUseSection> {
    let (first, rest) = path.split_first()?;
    if *first >= sections.len() {
        return None;
    }
    if rest.is_empty() {
        return Some(&mut sections[*first]);
    }
    section_mut(&mut sections[*first].children, rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FuzzRng(u64);

    impl FuzzRng {
        fn new(seed: u64) -> Self {
            Self(seed)
        }

        fn next_u32(&mut self) -> u32 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
            (self.0 >> 32) as u32
        }

        fn next_u8(&mut self) -> u8 {
            (self.next_u32() & 0xff) as u8
        }

        fn gen_ascii(&mut self, len: usize) -> String {
            let mut out = String::with_capacity(len);
            for _ in 0..len {
                let byte = (self.next_u8() % 95) + 0x20;
                out.push(byte as char);
            }
            out
        }
    }

    #[test]
    fn fuzz_parse_moveuse() {
        let mut rng = FuzzRng::new(0x0f00_d00d_cafe_beef);
        let mut content = String::new();
        content.push_str("BEGIN \"Root\"\nUse, Always -> Effect(15)\nEND\n");
        for _ in 0..150 {
            let len = (rng.next_u32() % 120) as usize;
            content.push_str(&rng.gen_ascii(len));
            content.push('\n');
        }
        let _ = parse_moveuse(&content);
    }
}
