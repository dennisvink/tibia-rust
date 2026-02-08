use crate::scripting::value::parse_value;
use crate::scripting::value::split_top_level;
use crate::scripting::value::ScriptValue;
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub struct NpcBehaviourRule {
    pub conditions: Vec<String>,
    pub actions: Vec<String>,
    pub line_no: usize,
    pub parsed_conditions: Vec<NpcCondition>,
    pub parsed_actions: Vec<NpcAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NpcCompareOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NpcCondition {
    Raw(String),
    Negation,
    String(String),
    Number(i64),
    Ident(String),
    Call { name: String, args: Vec<String> },
    Comparison {
        left: String,
        op: NpcCompareOp,
        right: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NpcAction {
    Raw(String),
    Say(String),
    Number(i64),
    Ident(String),
    Call { name: String, args: Vec<String> },
    Assignment { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpcTradeEntry {
    pub line_no: usize,
    pub conditions: Vec<String>,
    pub parsed_conditions: Vec<NpcCondition>,
    pub type_id: Option<i64>,
    pub amount: Option<String>,
    pub price: Option<String>,
    pub topic: Option<i64>,
    pub prompt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpcQuestRequirement {
    pub line_no: usize,
    pub quest_id: u16,
    pub op: NpcCompareOp,
    pub value: i64,
}

#[derive(Debug, Default)]
pub struct NpcScript {
    pub name: Option<String>,
    pub fields: Vec<(String, ScriptValue)>,
    pub behaviour: Vec<NpcBehaviourRule>,
    pub trade_entries: Vec<NpcTradeEntry>,
    pub quest_requirements: Vec<NpcQuestRequirement>,
}

impl NpcScript {
    pub fn field(&self, key: &str) -> Option<&ScriptValue> {
        self.fields
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(key))
            .map(|(_, value)| value)
    }
}

pub fn load_npc_script(path: &Path) -> Result<NpcScript, String> {
    let content = read_npc_script_text(path)?;
    let expanded = expand_npc_includes(path, &content)?;
    parse_npc_script(&expanded).map_err(|err| format!("npc script {}: {}", path.display(), err))
}

pub fn parse_npc_script(content: &str) -> Result<NpcScript, String> {
    let mut script = NpcScript::default();
    let mut in_behaviour = false;

    for (line_no, raw_line) in content.lines().enumerate() {
        let line_no = line_no + 1;
        let line = strip_inline_comment(raw_line);
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('@') {
            continue;
        }

        if in_behaviour {
            if line == "}" {
                in_behaviour = false;
                continue;
            }
            let Some((lhs, rhs)) = line.split_once("->") else {
                continue;
            };
            let conditions = split_top_level(lhs.trim(), ',')
                .map_err(|err| format!("line {} invalid conditions: {}", line_no, err))?;
            let actions = split_top_level(rhs.trim(), ',')
                .map_err(|err| format!("line {} invalid actions: {}", line_no, err))?;
            let rule = build_rule(conditions, actions, line_no);
            append_trade_entry(&mut script, &rule);
            append_quest_requirements(&mut script, &rule);
            script.behaviour.push(rule);
            continue;
        }

        if line.contains("->") {
            let (lhs, rhs) = line
                .split_once("->")
                .ok_or_else(|| format!("line {} missing '->' in behaviour", line_no))?;
            let conditions = split_top_level(lhs.trim(), ',')
                .map_err(|err| format!("line {} invalid conditions: {}", line_no, err))?;
            let actions = split_top_level(rhs.trim(), ',')
                .map_err(|err| format!("line {} invalid actions: {}", line_no, err))?;
            let rule = build_rule(conditions, actions, line_no);
            append_trade_entry(&mut script, &rule);
            append_quest_requirements(&mut script, &rule);
            script.behaviour.push(rule);
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim().to_string();
            let value = value.trim();
            if key.eq_ignore_ascii_case("Behaviour") || key.eq_ignore_ascii_case("Behavior") {
                if !value.starts_with('{') {
                    return Err(format!("line {} behaviour missing '{{'", line_no));
                }
                if value.trim() == "{" {
                    in_behaviour = true;
                } else if value.trim().ends_with('{') {
                    in_behaviour = true;
                } else if value.trim().ends_with("}") {
                    in_behaviour = false;
                } else {
                    in_behaviour = true;
                }
                continue;
            }

            let parsed = parse_value(value)
                .map_err(|err| format!("line {} invalid value: {}", line_no, err))?;
            if key.eq_ignore_ascii_case("Name") {
                if let ScriptValue::String(name) = &parsed {
                    script.name = Some(name.clone());
                }
            }
            script.fields.push((key, parsed));
            continue;
        }

        continue;
    }

    if in_behaviour {
        return Err("behaviour block not closed".to_string());
    }
    Ok(script)
}

fn read_npc_script_text(path: &Path) -> Result<String, String> {
    let bytes = std::fs::read(path)
        .map_err(|err| format!("failed to read npc script {}: {}", path.display(), err))?;
    Ok(match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(err) => {
            eprintln!(
                "tibia: npc script contained invalid UTF-8; decoding lossy: {}",
                path.display()
            );
            String::from_utf8_lossy(&err.into_bytes()).into_owned()
        }
    })
}

fn expand_npc_includes(path: &Path, content: &str) -> Result<String, String> {
    let mut stack = Vec::new();
    expand_npc_includes_inner(path, content, &mut stack)
}

fn expand_npc_includes_inner(
    path: &Path,
    content: &str,
    stack: &mut Vec<std::path::PathBuf>,
) -> Result<String, String> {
    stack.push(path.to_path_buf());
    let base_dir = path.parent().unwrap_or_else(|| Path::new(""));
    let mut output = String::new();
    for raw_line in content.lines() {
        let trimmed = strip_inline_comment(raw_line);
        let trimmed = trimmed.trim();
        if let Some(target) = parse_include_target(trimmed) {
            let include_path = base_dir.join(&target);
            if stack.iter().any(|entry| entry == &include_path) {
                return Err(format!(
                    "npc script include cycle: {} -> {}",
                    path.display(),
                    include_path.display()
                ));
            }
            let include_content = read_npc_script_text(&include_path)?;
            let expanded = expand_npc_includes_inner(&include_path, &include_content, stack)?;
            output.push_str(&expanded);
            if !expanded.ends_with('\n') {
                output.push('\n');
            }
            continue;
        }
        output.push_str(raw_line);
        output.push('\n');
    }
    stack.pop();
    Ok(output)
}

fn parse_include_target(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if !trimmed.starts_with('@') {
        return None;
    }
    let rest = trimmed[1..].trim();
    if rest.is_empty() {
        return None;
    }
    let mut chars = rest.chars();
    let quote = chars.next()?;
    if quote == '"' || quote == '\'' {
        let remainder = &rest[1..];
        let end = remainder.find(quote)?;
        return Some(remainder[..end].to_string());
    }
    Some(rest.split_whitespace().next()?.to_string())
}

fn strip_inline_comment(line: &str) -> String {
    let mut in_quotes = false;
    for (idx, ch) in line.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            '#' if !in_quotes => return line[..idx].to_string(),
            _ => {}
        }
    }
    line.to_string()
}

fn build_rule(conditions: Vec<String>, actions: Vec<String>, line_no: usize) -> NpcBehaviourRule {
    let parsed_conditions = conditions
        .iter()
        .map(|condition| parse_condition_token(condition))
        .collect();
    let parsed_actions = actions
        .iter()
        .map(|action| parse_action_token(action))
        .collect();
    NpcBehaviourRule {
        conditions,
        actions,
        line_no,
        parsed_conditions,
        parsed_actions,
    }
}

fn parse_condition_token(raw: &str) -> NpcCondition {
    let token = raw.trim();
    if token == "!" {
        return NpcCondition::Negation;
    }
    if let Some(value) = parse_quoted(token) {
        return NpcCondition::String(value);
    }
    if let Some((left, op, right)) = parse_comparison(token) {
        return NpcCondition::Comparison { left, op, right };
    }
    if let Some((name, args)) = parse_call(token) {
        return NpcCondition::Call { name, args };
    }
    if let Ok(number) = token.parse::<i64>() {
        return NpcCondition::Number(number);
    }
    if token.is_empty() {
        return NpcCondition::Raw(raw.to_string());
    }
    NpcCondition::Ident(token.to_string())
}

fn parse_action_token(raw: &str) -> NpcAction {
    let token = raw.trim();
    if let Some(value) = parse_quoted(token) {
        return NpcAction::Say(value);
    }
    if let Some((name, args)) = parse_call(token) {
        return NpcAction::Call { name, args };
    }
    if let Some((key, value)) = parse_assignment(token) {
        return NpcAction::Assignment { key, value };
    }
    if let Ok(number) = token.parse::<i64>() {
        return NpcAction::Number(number);
    }
    if token.is_empty() {
        return NpcAction::Raw(raw.to_string());
    }
    NpcAction::Ident(token.to_string())
}

fn parse_assignment(token: &str) -> Option<(String, String)> {
    if token.contains('>') || token.contains('<') {
        return None;
    }
    let (key, value) = token.split_once('=')?;
    let key = key.trim();
    let value = value.trim();
    if key.is_empty() || value.is_empty() {
        return None;
    }
    Some((key.to_string(), value.to_string()))
}

fn parse_comparison(token: &str) -> Option<(String, NpcCompareOp, String)> {
    for (op_str, op) in [
        ("<>", NpcCompareOp::Ne),
        ("!=", NpcCompareOp::Ne),
        (">=", NpcCompareOp::Ge),
        ("<=", NpcCompareOp::Le),
        (">", NpcCompareOp::Gt),
        ("<", NpcCompareOp::Lt),
        ("=", NpcCompareOp::Eq),
    ] {
        if let Some(idx) = token.find(op_str) {
            let left = token[..idx].trim();
            let right = token[idx + op_str.len()..].trim();
            if left.is_empty() || right.is_empty() {
                continue;
            }
            return Some((left.to_string(), op.clone(), right.to_string()));
        }
    }
    None
}

fn parse_call(token: &str) -> Option<(String, Vec<String>)> {
    let open = token.find('(')?;
    if !token.ends_with(')') {
        return None;
    }
    let name = token[..open].trim();
    if name.is_empty() {
        return None;
    }
    let inner = &token[open + 1..token.len() - 1];
    if inner.trim().is_empty() {
        return Some((name.to_string(), Vec::new()));
    }
    let args = match split_top_level(inner, ',') {
        Ok(args) => args,
        Err(_) => return None,
    };
    Some((name.to_string(), args))
}

fn parse_quoted(token: &str) -> Option<String> {
    if token.starts_with('"') && token.ends_with('"') && token.len() >= 2 {
        return Some(token[1..token.len() - 1].to_string());
    }
    None
}

fn append_trade_entry(script: &mut NpcScript, rule: &NpcBehaviourRule) {
    let mut type_id = None;
    let mut amount = None;
    let mut price = None;
    let mut topic = None;
    let mut prompt = None;

    for action in &rule.parsed_actions {
        match action {
            NpcAction::Assignment { key, value } if key.eq_ignore_ascii_case("Type") => {
                type_id = value.parse::<i64>().ok();
            }
            NpcAction::Assignment { key, value } if key.eq_ignore_ascii_case("Amount") => {
                amount = Some(value.clone());
            }
            NpcAction::Assignment { key, value } if key.eq_ignore_ascii_case("Price") => {
                price = Some(value.clone());
            }
            NpcAction::Assignment { key, value } if key.eq_ignore_ascii_case("Topic") => {
                topic = value.parse::<i64>().ok();
            }
            NpcAction::Say(text) => {
                if prompt.is_none() {
                    prompt = Some(text.clone());
                }
            }
            _ => {}
        }
    }

    if type_id.is_some() || price.is_some() || amount.is_some() {
        script.trade_entries.push(NpcTradeEntry {
            line_no: rule.line_no,
            conditions: rule.conditions.clone(),
            parsed_conditions: rule.parsed_conditions.clone(),
            type_id,
            amount,
            price,
            topic,
            prompt,
        });
    }
}

fn append_quest_requirements(script: &mut NpcScript, rule: &NpcBehaviourRule) {
    for condition in &rule.parsed_conditions {
        let NpcCondition::Comparison { left, op, right } = condition else {
            continue;
        };
        if !left.to_ascii_lowercase().starts_with("questvalue(") {
            continue;
        }
        let id_str = left
            .trim_start_matches(|ch: char| ch != '(')
            .trim_start_matches('(')
            .trim_end_matches(')');
        let quest_id = match id_str.trim().parse::<u16>() {
            Ok(id) => id,
            Err(_) => continue,
        };
        let value = match right.trim().parse::<i64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        script.quest_requirements.push(NpcQuestRequirement {
            line_no: rule.line_no,
            quest_id,
            op: op.clone(),
            value,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_condition_comparison() {
        let parsed = parse_condition_token("QuestValue(250)>2");
        assert_eq!(
            parsed,
            NpcCondition::Comparison {
                left: "QuestValue(250)".to_string(),
                op: NpcCompareOp::Gt,
                right: "2".to_string(),
            }
        );
    }

    #[test]
    fn parse_action_assignment_and_prompt() {
        let actions = vec![
            "Type=3277".to_string(),
            "Price=1".to_string(),
            "\"Do you want to buy?\"".to_string(),
        ];
        let rule = build_rule(Vec::new(), actions, 10);
        let mut script = NpcScript::default();
        append_trade_entry(&mut script, &rule);
        assert_eq!(script.trade_entries.len(), 1);
        let entry = &script.trade_entries[0];
        assert_eq!(entry.type_id, Some(3277));
        assert_eq!(entry.price.as_deref(), Some("1"));
        assert_eq!(entry.prompt.as_deref(), Some("Do you want to buy?"));
    }

    #[test]
    fn parse_npc_script_collects_behaviour_trade_and_quest() {
        let input = r#"
Name = "Klara"
Behaviour = {
"hi" -> "Hello there."
QuestValue(250) >= 2 -> "You may pass."
Topic=1 -> Type=3031, Amount=2, Price=10, Topic=1, "Do you want to buy?"
}
"#;
        let script = parse_npc_script(input).expect("parse script");
        assert_eq!(script.name.as_deref(), Some("Klara"));
        assert_eq!(script.behaviour.len(), 3);
        assert_eq!(script.trade_entries.len(), 1);
        let entry = &script.trade_entries[0];
        assert_eq!(entry.type_id, Some(3031));
        assert_eq!(entry.amount.as_deref(), Some("2"));
        assert_eq!(entry.price.as_deref(), Some("10"));
        assert_eq!(entry.topic, Some(1));
        assert_eq!(entry.prompt.as_deref(), Some("Do you want to buy?"));
        assert_eq!(entry.conditions, vec!["Topic=1"]);

        assert_eq!(script.quest_requirements.len(), 1);
        let quest = &script.quest_requirements[0];
        assert_eq!(quest.quest_id, 250);
        assert_eq!(quest.op, NpcCompareOp::Ge);
        assert_eq!(quest.value, 2);
    }
}
