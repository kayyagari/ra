use std::borrow::Cow;
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char;

pub fn remove_diacritics_and_multi_spaces<'i, S: Into<Cow<'i, str>>>(input: S) -> Cow<'i, str> {
    // Normalization Form KD (NFKD) ==> Compatibility Decomposition
    let input = input.into();
    let mut has_non_white_space = false;
    let mut prev = '\n';

    let mut modified: Option<String> = None;

    for (pos, c) in input.chars().enumerate() {
        match c {
            ' ' | '\t' => {
                if prev == ' ' {
                    if let None = modified {
                        let mut tmp = String::with_capacity(input.len());
                        if has_non_white_space {
                            tmp.push_str(&input[..pos]);
                        }
                        modified = Some(tmp);
                    }
                }
                else if let Some(ref mut tmp) = modified {
                    tmp.push(c);
                }
                prev = ' ';
            },
            _ => {
                if !c.is_ascii() {
                    if let None = modified {
                        let mut tmp = String::with_capacity(input.len());
                        if pos > 0 {
                            tmp.push_str(&input[..pos]);
                        }
                        modified = Some(tmp);
                    }
                    let emit_char = |ec| {
                        if !char::is_combining_mark(ec) {
                            modified.as_mut().unwrap().push(ec);
                        }
                    };
                    char::decompose_compatible(c, emit_char);
                }
                else if let Some(ref mut tmp) = modified {
                    tmp.push(c);
                }

                has_non_white_space = true;
                prev = c;
            }
        }
    }

    let mut output;
    if let Some(s) = modified {
        output = Cow::Owned(s);
    }
    else {
        output = input;
    }

    output
}

fn replace_multiple_spaces(input: &str) -> String {
    let mut s = String::with_capacity(input.len());
    let mut prev = '\n';
    for c in input.chars() {
        match c {
            ' ' | '\t' => {
                if prev != ' ' {
                    s.push(' ');
                }

                prev = ' ';
            },
            _ => {
                s.push(c);
                prev = c;
            }
        }
    }

    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize() {
        // let emit_char = |c| {
        //     if !char::is_combining_mark(c)
        //     {
        //         println!("--> {}", c);
        //     }
        //     println!("xx> {}", c);
        // };
        // char::decompose_canonical('Å', emit_char);
        let mut candidates = Vec::new();
        candidates.push(("this     line has 		unicode ÅΩ", "this line has unicode AΩ"));
        candidates.push(("	 tHis     line has 		multiple spaces", "tHis line has multiple spaces"));
        candidates.push(("  ", ""));
        candidates.push((" 	 	 ", ""));
        candidates.push(("", ""));
        candidates.push(("ß Ç i⁹ i₉ starts with unicode and has trailing spaces  	 ", "ß C i9 i9 starts with unicode and has trailing spaces "));
        candidates.push(("ends with ß Ç i⁹ i₉", "ends with ß C i9 i9"));
        candidates.push(("just a plain string", "just a plain string"));

        for (input, expected) in candidates {
            let actual = remove_diacritics_and_multi_spaces(input);
            assert_eq!(expected, actual);
        }
    }
}