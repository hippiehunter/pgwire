//! Parser for PostgreSQL replication commands.
//!
//! Replication commands are sent as simple `Query` messages but have special
//! syntax. This parser recognizes them via case-insensitive prefix matching
//! and returns `None` for anything that doesn't match, allowing fallthrough
//! to the regular query handler.

use crate::messages::replication::Lsn;

/// Type of a replication slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotType {
    Physical,
    Logical,
}

/// The START_REPLICATION command with all its parameters.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartReplicationCommand {
    /// Slot name (optional for physical replication).
    pub slot_name: Option<String>,
    /// Whether this is logical replication.
    pub logical: bool,
    /// Starting LSN.
    pub start_lsn: Lsn,
    /// Timeline (physical replication only).
    pub timeline: Option<u32>,
    /// Plugin options (logical replication).
    pub options: Vec<(String, Option<String>)>,
}

/// Parsed replication command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationCommand {
    IdentifySystem,
    TimelineHistory {
        timeline: u32,
    },
    CreateReplicationSlot {
        slot_name: String,
        temporary: bool,
        slot_type: SlotType,
        options: Vec<(String, Option<String>)>,
    },
    DropReplicationSlot {
        slot_name: String,
        wait: bool,
    },
    ReadReplicationSlot {
        slot_name: String,
    },
    StartReplication(StartReplicationCommand),
    AlterReplicationSlot {
        slot_name: String,
        options: Vec<(String, Option<String>)>,
    },
}

/// Parse a replication command from a SQL query string.
///
/// Returns `None` if the query is not a replication command (i.e., it's regular
/// SQL that should be handled by the SimpleQueryHandler).
pub fn parse_replication_command(query: &str) -> Option<ReplicationCommand> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    let mut tokens = Tokenizer::new(trimmed);

    let first = tokens.next_token()?;
    match first.to_ascii_uppercase().as_str() {
        "IDENTIFY_SYSTEM" => Some(ReplicationCommand::IdentifySystem),
        "TIMELINE_HISTORY" => {
            let tl_str = tokens.next_token()?;
            let timeline: u32 = tl_str.parse().ok()?;
            Some(ReplicationCommand::TimelineHistory { timeline })
        }
        "CREATE_REPLICATION_SLOT" => parse_create_replication_slot(&mut tokens),
        "DROP_REPLICATION_SLOT" => parse_drop_replication_slot(&mut tokens),
        "READ_REPLICATION_SLOT" => {
            let slot_name = tokens.next_token()?;
            Some(ReplicationCommand::ReadReplicationSlot {
                slot_name: unquote(&slot_name),
            })
        }
        "START_REPLICATION" => parse_start_replication(&mut tokens),
        "ALTER_REPLICATION_SLOT" => parse_alter_replication_slot(&mut tokens),
        _ => None,
    }
}

fn parse_create_replication_slot(tokens: &mut Tokenizer) -> Option<ReplicationCommand> {
    let slot_name = unquote(&tokens.next_token()?);

    let mut temporary = false;
    let mut slot_type = None;

    while let Some(tok) = tokens.next_token() {
        match tok.to_ascii_uppercase().as_str() {
            "TEMPORARY" => temporary = true,
            "PHYSICAL" => slot_type = Some(SlotType::Physical),
            "LOGICAL" => {
                slot_type = Some(SlotType::Logical);
                break;
            }
            _ => break,
        }
    }

    // For LOGICAL, the next token is the output plugin name (treated as first option value)
    let mut options = Vec::new();
    if slot_type == Some(SlotType::Logical) {
        // Next token is the output plugin name
        if let Some(plugin) = tokens.next_token() {
            options.push(("output_plugin".to_owned(), Some(unquote(&plugin))));
        }
    }

    // Parse remaining options in parentheses
    if let Some(paren_opts) = tokens.rest().trim().strip_prefix('(') {
        let paren_opts = paren_opts.trim_end_matches(')').trim();
        options.extend(parse_option_list(paren_opts));
    }

    Some(ReplicationCommand::CreateReplicationSlot {
        slot_name,
        temporary,
        slot_type: slot_type.unwrap_or(SlotType::Physical),
        options,
    })
}

fn parse_drop_replication_slot(tokens: &mut Tokenizer) -> Option<ReplicationCommand> {
    let slot_name = unquote(&tokens.next_token()?);
    let mut wait = false;
    if let Some(tok) = tokens.next_token()
        && tok.eq_ignore_ascii_case("WAIT")
    {
        wait = true;
    }
    Some(ReplicationCommand::DropReplicationSlot { slot_name, wait })
}

fn parse_start_replication(tokens: &mut Tokenizer) -> Option<ReplicationCommand> {
    let mut slot_name = None;
    let mut logical = false;

    // START_REPLICATION [SLOT slot_name] [LOGICAL] lsn [TIMELINE tl] [(options)]
    let tok = tokens.next_token()?;
    let upper = tok.to_ascii_uppercase();

    let lsn_str;
    if upper == "SLOT" {
        slot_name = Some(unquote(&tokens.next_token()?));
        let next = tokens.next_token()?;
        let next_upper = next.to_ascii_uppercase();
        if next_upper == "LOGICAL" {
            logical = true;
            lsn_str = tokens.next_token()?;
        } else {
            lsn_str = next;
        }
    } else if upper == "LOGICAL" {
        logical = true;
        lsn_str = tokens.next_token()?;
    } else {
        lsn_str = tok;
    }

    let start_lsn: Lsn = lsn_str.parse().ok()?;

    let mut timeline = None;
    let mut options = Vec::new();

    let rest = tokens.rest().trim();
    if !rest.is_empty() {
        // Check for TIMELINE or parenthesized options
        let mut rest_tokens = Tokenizer::new(rest);
        while let Some(rtok) = rest_tokens.next_token() {
            let rtok_upper = rtok.to_ascii_uppercase();
            if rtok_upper == "TIMELINE" {
                if let Some(tl_str) = rest_tokens.next_token() {
                    timeline = tl_str.parse().ok();
                }
            } else if rtok.starts_with('(') {
                // Options in parentheses
                let opt_str = format!("{} {}", rtok, rest_tokens.rest());
                let opt_str = opt_str
                    .trim()
                    .trim_start_matches('(')
                    .trim_end_matches(')')
                    .trim();
                options = parse_option_list(opt_str);
                break;
            }
        }
    }

    Some(ReplicationCommand::StartReplication(
        StartReplicationCommand {
            slot_name,
            logical,
            start_lsn,
            timeline,
            options,
        },
    ))
}

fn parse_alter_replication_slot(tokens: &mut Tokenizer) -> Option<ReplicationCommand> {
    let slot_name = unquote(&tokens.next_token()?);
    let mut options = Vec::new();

    let rest = tokens.rest().trim();
    if let Some(paren_opts) = rest.strip_prefix('(') {
        let paren_opts = paren_opts.trim_end_matches(')').trim();
        options = parse_option_list(paren_opts);
    }

    Some(ReplicationCommand::AlterReplicationSlot { slot_name, options })
}

/// Parse a comma-separated option list like `key1 'value1', key2 'value2'`.
fn parse_option_list(s: &str) -> Vec<(String, Option<String>)> {
    let mut options = Vec::new();
    if s.is_empty() {
        return options;
    }

    for part in split_top_level_commas(s) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut t = Tokenizer::new(part);
        if let Some(key) = t.next_token() {
            let value = t.next_token().map(|v| unquote(&v));
            options.push((key.to_ascii_lowercase(), value));
        }
    }
    options
}

/// Remove surrounding quotes from an identifier or string literal.
fn unquote(s: &str) -> String {
    if s.len() >= 2
        && ((s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')))
    {
        s[1..s.len() - 1].to_owned()
    } else {
        s.to_owned()
    }
}

fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut idx = 0usize;
    let mut paren_depth = 0usize;
    let mut quote: Option<u8> = None;
    let bytes = s.as_bytes();

    while idx < bytes.len() {
        let byte = bytes[idx];
        if let Some(active_quote) = quote {
            if byte == active_quote {
                if idx + 1 < bytes.len() && bytes[idx + 1] == active_quote {
                    idx += 2;
                    continue;
                }
                quote = None;
            }
            idx += 1;
            continue;
        }

        match byte {
            b'\'' | b'"' => {
                quote = Some(byte);
            }
            b'(' => paren_depth += 1,
            b')' => paren_depth = paren_depth.saturating_sub(1),
            b',' if paren_depth == 0 => {
                parts.push(&s[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
        idx += 1;
    }

    parts.push(&s[start..]);
    parts
}

/// Simple whitespace-aware tokenizer that respects quoted strings.
struct Tokenizer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    fn new(input: &'a str) -> Self {
        Tokenizer { input, pos: 0 }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input.as_bytes()[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn next_token(&mut self) -> Option<String> {
        self.skip_whitespace();
        if self.pos >= self.input.len() {
            return None;
        }

        let start = self.pos;
        let ch = self.input.as_bytes()[self.pos];

        if ch == b'\'' || ch == b'"' {
            // Quoted string
            self.pos += 1;
            while self.pos < self.input.len() && self.input.as_bytes()[self.pos] != ch {
                self.pos += 1;
            }
            if self.pos < self.input.len() {
                self.pos += 1; // skip closing quote
            }
            Some(self.input[start..self.pos].to_owned())
        } else {
            // Unquoted token: read until whitespace or special char
            while self.pos < self.input.len() {
                let b = self.input.as_bytes()[self.pos];
                if b.is_ascii_whitespace() || b == b',' {
                    break;
                }
                self.pos += 1;
            }
            Some(self.input[start..self.pos].to_owned())
        }
    }

    fn rest(&self) -> &'a str {
        &self.input[self.pos..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identify_system() {
        let cmd = parse_replication_command("IDENTIFY_SYSTEM").unwrap();
        assert_eq!(cmd, ReplicationCommand::IdentifySystem);
    }

    #[test]
    fn test_identify_system_case_insensitive() {
        let cmd = parse_replication_command("identify_system").unwrap();
        assert_eq!(cmd, ReplicationCommand::IdentifySystem);
    }

    #[test]
    fn test_identify_system_with_semicolon() {
        let cmd = parse_replication_command("IDENTIFY_SYSTEM;").unwrap();
        assert_eq!(cmd, ReplicationCommand::IdentifySystem);
    }

    #[test]
    fn test_timeline_history() {
        let cmd = parse_replication_command("TIMELINE_HISTORY 3").unwrap();
        assert_eq!(cmd, ReplicationCommand::TimelineHistory { timeline: 3 });
    }

    #[test]
    fn test_create_replication_slot_physical() {
        let cmd = parse_replication_command("CREATE_REPLICATION_SLOT myslot PHYSICAL").unwrap();
        match cmd {
            ReplicationCommand::CreateReplicationSlot {
                slot_name,
                temporary,
                slot_type,
                ..
            } => {
                assert_eq!(slot_name, "myslot");
                assert!(!temporary);
                assert_eq!(slot_type, SlotType::Physical);
            }
            _ => panic!("expected CreateReplicationSlot"),
        }
    }

    #[test]
    fn test_create_replication_slot_logical() {
        let cmd =
            parse_replication_command("CREATE_REPLICATION_SLOT myslot LOGICAL pgoutput").unwrap();
        match cmd {
            ReplicationCommand::CreateReplicationSlot {
                slot_name,
                slot_type,
                options,
                ..
            } => {
                assert_eq!(slot_name, "myslot");
                assert_eq!(slot_type, SlotType::Logical);
                assert_eq!(
                    options,
                    vec![("output_plugin".to_owned(), Some("pgoutput".to_owned()))]
                );
            }
            _ => panic!("expected CreateReplicationSlot"),
        }
    }

    #[test]
    fn test_create_replication_slot_temporary() {
        let cmd = parse_replication_command(
            "CREATE_REPLICATION_SLOT myslot TEMPORARY LOGICAL test_decoding",
        )
        .unwrap();
        match cmd {
            ReplicationCommand::CreateReplicationSlot {
                slot_name,
                temporary,
                slot_type,
                ..
            } => {
                assert_eq!(slot_name, "myslot");
                assert!(temporary);
                assert_eq!(slot_type, SlotType::Logical);
            }
            _ => panic!("expected CreateReplicationSlot"),
        }
    }

    #[test]
    fn test_drop_replication_slot() {
        let cmd = parse_replication_command("DROP_REPLICATION_SLOT myslot").unwrap();
        match cmd {
            ReplicationCommand::DropReplicationSlot { slot_name, wait } => {
                assert_eq!(slot_name, "myslot");
                assert!(!wait);
            }
            _ => panic!("expected DropReplicationSlot"),
        }
    }

    #[test]
    fn test_drop_replication_slot_wait() {
        let cmd = parse_replication_command("DROP_REPLICATION_SLOT myslot WAIT").unwrap();
        match cmd {
            ReplicationCommand::DropReplicationSlot { slot_name, wait } => {
                assert_eq!(slot_name, "myslot");
                assert!(wait);
            }
            _ => panic!("expected DropReplicationSlot"),
        }
    }

    #[test]
    fn test_read_replication_slot() {
        let cmd = parse_replication_command("READ_REPLICATION_SLOT myslot").unwrap();
        assert_eq!(
            cmd,
            ReplicationCommand::ReadReplicationSlot {
                slot_name: "myslot".to_owned()
            }
        );
    }

    #[test]
    fn test_start_replication_physical() {
        let cmd = parse_replication_command("START_REPLICATION 0/1000000").unwrap();
        match cmd {
            ReplicationCommand::StartReplication(sr) => {
                assert!(!sr.logical);
                assert_eq!(sr.start_lsn, Lsn(0x0100_0000));
                assert!(sr.slot_name.is_none());
                assert!(sr.timeline.is_none());
            }
            _ => panic!("expected StartReplication"),
        }
    }

    #[test]
    fn test_start_replication_physical_with_timeline() {
        let cmd = parse_replication_command("START_REPLICATION 0/1000000 TIMELINE 2").unwrap();
        match cmd {
            ReplicationCommand::StartReplication(sr) => {
                assert!(!sr.logical);
                assert_eq!(sr.start_lsn, Lsn(0x0100_0000));
                assert_eq!(sr.timeline, Some(2));
            }
            _ => panic!("expected StartReplication"),
        }
    }

    #[test]
    fn test_start_replication_logical_with_slot() {
        let cmd = parse_replication_command(
            "START_REPLICATION SLOT myslot LOGICAL 0/1000000 (proto_version '1', publication_names 'pub1')",
        )
        .unwrap();
        match cmd {
            ReplicationCommand::StartReplication(sr) => {
                assert!(sr.logical);
                assert_eq!(sr.slot_name.as_deref(), Some("myslot"));
                assert_eq!(sr.start_lsn, Lsn(0x0100_0000));
                assert_eq!(sr.options.len(), 2);
                assert_eq!(sr.options[0].0, "proto_version");
                assert_eq!(sr.options[0].1.as_deref(), Some("1"));
                assert_eq!(sr.options[1].0, "publication_names");
                assert_eq!(sr.options[1].1.as_deref(), Some("pub1"));
            }
            _ => panic!("expected StartReplication"),
        }
    }

    #[test]
    fn test_start_replication_logical_with_multiple_publications() {
        let cmd = parse_replication_command(
            "START_REPLICATION SLOT myslot LOGICAL 0/1000000 (proto_version '1', publication_names 'pub1, pub2')",
        )
        .unwrap();
        match cmd {
            ReplicationCommand::StartReplication(sr) => {
                assert!(sr.logical);
                assert_eq!(sr.slot_name.as_deref(), Some("myslot"));
                assert_eq!(sr.start_lsn, Lsn(0x0100_0000));
                assert_eq!(sr.options.len(), 2);
                assert_eq!(sr.options[0].0, "proto_version");
                assert_eq!(sr.options[0].1.as_deref(), Some("1"));
                assert_eq!(sr.options[1].0, "publication_names");
                assert_eq!(sr.options[1].1.as_deref(), Some("pub1, pub2"));
            }
            _ => panic!("expected StartReplication"),
        }
    }

    #[test]
    fn test_parse_option_list_preserves_commas_inside_quotes() {
        let options =
            parse_option_list("proto_version '1', publication_names 'pub1, pub2', messages 'true'");
        assert_eq!(
            options,
            vec![
                ("proto_version".to_owned(), Some("1".to_owned())),
                (
                    "publication_names".to_owned(),
                    Some("pub1, pub2".to_owned())
                ),
                ("messages".to_owned(), Some("true".to_owned())),
            ]
        );
    }

    #[test]
    fn test_start_replication_slot_physical() {
        let cmd = parse_replication_command("START_REPLICATION SLOT myslot 0/1000000").unwrap();
        match cmd {
            ReplicationCommand::StartReplication(sr) => {
                assert!(!sr.logical);
                assert_eq!(sr.slot_name.as_deref(), Some("myslot"));
                assert_eq!(sr.start_lsn, Lsn(0x0100_0000));
            }
            _ => panic!("expected StartReplication"),
        }
    }

    #[test]
    fn test_regular_sql_returns_none() {
        assert!(parse_replication_command("SELECT 1").is_none());
        assert!(parse_replication_command("INSERT INTO t VALUES (1)").is_none());
        assert!(parse_replication_command("BEGIN").is_none());
    }

    #[test]
    fn test_alter_replication_slot() {
        let cmd = parse_replication_command(
            "ALTER_REPLICATION_SLOT myslot (failover true, two_phase false)",
        )
        .unwrap();
        match cmd {
            ReplicationCommand::AlterReplicationSlot { slot_name, options } => {
                assert_eq!(slot_name, "myslot");
                assert_eq!(options.len(), 2);
                assert_eq!(options[0], ("failover".to_owned(), Some("true".to_owned())));
                assert_eq!(
                    options[1],
                    ("two_phase".to_owned(), Some("false".to_owned()))
                );
            }
            _ => panic!("expected AlterReplicationSlot"),
        }
    }

    #[test]
    fn test_quoted_slot_name() {
        let cmd = parse_replication_command(r#"READ_REPLICATION_SLOT "my slot""#).unwrap();
        assert_eq!(
            cmd,
            ReplicationCommand::ReadReplicationSlot {
                slot_name: "my slot".to_owned()
            }
        );
    }

    #[test]
    fn test_whitespace_variations() {
        let cmd = parse_replication_command("  IDENTIFY_SYSTEM  ").unwrap();
        assert_eq!(cmd, ReplicationCommand::IdentifySystem);
    }
}
