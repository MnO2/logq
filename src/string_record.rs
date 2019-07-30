use regex::Regex;

#[derive(Debug)]
pub struct StringRecord {
    pub fields: String,
    split_the_line_regex: Regex,
}

impl StringRecord {
    pub fn new() -> Self {
        let regex_literal = r#"[^\s"']+|"([^"]*)"|'([^']*)'"#;
        let split_the_line_regex: Regex = Regex::new(regex_literal).unwrap();

        StringRecord {
            fields: String::new(),
            split_the_line_regex,
        }
    }

    pub fn get(&self, i: usize) -> Option<&str> {
        let r: Vec<&str> = self
            .split_the_line_regex
            .find_iter(&self.fields)
            .map(|x| x.as_str())
            .collect();

        if i < r.len() {
            Some(r[i])
        } else {
            None
        }
    }
}
