//! Identifier substitution shared by [`DofConstraint`](super::dof::DofConstraint),
//! [`OutcomeConstraint`](super::objective::OutcomeConstraint) and
//! [`ScalarizedObjective`](super::objective::ScalarizedObjective) — blop's
//! `re.sub(rf"\b{key}\b", "{key}", template).format(**names)`.
//!
//! A template variable is a maximal run of word characters (`[A-Za-z0-9_]`),
//! which is what Python's `\b…\b` delimits. Validation and substitution use
//! the same tokenizer, so a variable that validates is the one substituted;
//! blop validates with a substring test, which accepts `x` against `x1 + x2`
//! and then substitutes nothing.

fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// The word tokens of `template`, in order, with repeats.
pub(super) fn words(template: &str) -> impl Iterator<Item = &str> {
    template
        .split(|c: char| !is_word(c))
        .filter(|w| !w.is_empty())
}

/// Whether `name` occurs in `template` as a whole word.
pub(super) fn contains_word(template: &str, name: &str) -> bool {
    words(template).any(|w| w == name)
}

/// `template` with every whole-word occurrence of a key replaced by
/// `lookup(key)`; words `lookup` does not know stay as they are.
pub(super) fn substitute(template: &str, lookup: impl Fn(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut word = String::new();
    let flush = |word: &mut String, out: &mut String| {
        if !word.is_empty() {
            match lookup(word) {
                Some(replacement) => out.push_str(&replacement),
                None => out.push_str(word),
            }
            word.clear();
        }
    };
    for c in template.chars() {
        if is_word(c) {
            word.push(c);
        } else {
            flush(&mut word, &mut out);
            out.push(c);
        }
    }
    flush(&mut word, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_words_only() {
        let lookup = |w: &str| match w {
            "x" => Some("motor_x".to_owned()),
            "x1" => Some("m1".to_owned()),
            _ => None,
        };
        assert_eq!(
            substitute("x + x1 <= ax + 2*x", lookup),
            "motor_x + m1 <= ax + 2*motor_x"
        );
        assert!(contains_word("x1 + x2 <= 10", "x1"));
        assert!(!contains_word("x1 + x2 <= 10", "x"));
        assert_eq!(words("-(a + 2 * b)").collect::<Vec<_>>(), ["a", "2", "b"]);
    }
}
