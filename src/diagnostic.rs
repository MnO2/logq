pub(crate) fn render(query: &str, offset: usize, message: &str, label: &str, hint: Option<&str>) -> String {
    let offset = offset.min(query.len());
    let line_start = query[..offset].rfind('\n').map_or(0, |position| position + 1);
    let line_end = query[offset..]
        .find('\n')
        .map_or(query.len(), |position| offset + position);
    let line_number = query[..line_start].bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = query[line_start..offset].chars().count();
    let source_line = &query[line_start..line_end];
    let underline_len = token_len(&query[offset..line_end]).max(1);
    let gutter_width = line_number.to_string().len();

    let mut rendered = format!(
        "error: {message}\n  --> query:{line_number}:{}\n{empty:>width$} |\n{line_number:>width$} | {source_line}\n{empty:>width$} | {padding}^{underline} {label}",
        column + 1,
        empty = "",
        width = gutter_width,
        padding = " ".repeat(column),
        underline = "~".repeat(underline_len.saturating_sub(1)),
    );
    if let Some(hint) = hint {
        rendered.push_str(&format!(
            "\n{empty:>width$} = hint: {hint}",
            empty = "",
            width = gutter_width
        ));
    }
    rendered
}

fn token_len(input: &str) -> usize {
    let mut chars = input.chars();
    let Some(first) = chars.next() else {
        return 0;
    };
    if first.is_alphanumeric() || first == '_' {
        1 + chars.take_while(|ch| ch.is_alphanumeric() || *ch == '_').count()
    } else if "=<>!|+-*/".contains(first) {
        1 + chars.take_while(|ch| "=<>!|+-*/".contains(*ch)).count()
    } else {
        1
    }
}

pub(crate) fn suggestion<'a, I>(input: &str, candidates: I) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    let normalized = input.to_ascii_lowercase();
    candidates
        .into_iter()
        .map(|candidate| (levenshtein(&normalized, &candidate.to_ascii_lowercase()), candidate))
        .filter(|(distance, candidate)| *distance <= suggestion_threshold(normalized.len(), candidate.len()))
        .min_by_key(|(distance, candidate)| (*distance, candidate.len()))
        .map(|(_, candidate)| candidate)
}

fn suggestion_threshold(input_len: usize, candidate_len: usize) -> usize {
    match input_len.max(candidate_len) {
        0..=4 => 1,
        5..=8 => 2,
        _ => 3,
    }
}

fn levenshtein(left: &str, right: &str) -> usize {
    let mut previous: Vec<usize> = (0..=right.chars().count()).collect();
    let mut current = vec![0; previous.len()];
    for (left_index, left_char) in left.chars().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_char) in right.chars().enumerate() {
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + usize::from(left_char != right_char));
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.chars().count()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_multiline_byte_offsets_as_character_columns() {
        let query = "select *\nfrom café";
        let offset = query.find("café").unwrap();
        let rendered = render(query, offset, "bad table", "unknown table", Some("check the name"));
        assert!(rendered.contains("query:2:6"));
        assert!(rendered.contains("2 | from café"));
        assert!(rendered.contains("|      ^~~~ unknown table"));
        assert!(rendered.contains("= hint: check the name"));
    }

    #[test]
    fn suggests_only_nearby_names() {
        assert_eq!(suggestion("selec", ["select", "from"]), Some("select"));
        assert_eq!(suggestion("completely_different", ["select", "from"]), None);
    }
}
