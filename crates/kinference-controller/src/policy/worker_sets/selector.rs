pub(super) fn valid(selector: &str) -> bool {
    if selector.chars().any(|character| {
        character.is_whitespace() && !matches!(character, ' ' | '\t' | '\r' | '\n')
    }) {
        return false;
    }
    if selector.trim().is_empty() {
        return true;
    }
    let mut start = 0;
    let mut in_set = false;
    for (offset, character) in selector.char_indices() {
        match character {
            '(' if !in_set => in_set = true,
            ')' if in_set => in_set = false,
            '(' | ')' => return false,
            ',' if !in_set => {
                if !requirement(&selector[start..offset]) {
                    return false;
                }
                start = offset + 1;
            }
            _ => {}
        }
    }
    !in_set && requirement(&selector[start..])
}

fn requirement(requirement: &str) -> bool {
    let requirement = requirement.trim();
    if let Some(key) = requirement.strip_prefix('!') {
        return valid_key(key.trim());
    }
    if let Some((head, values)) = requirement.split_once('(') {
        let mut words = head.split_whitespace();
        let Some(key) = words.next() else {
            return false;
        };
        let Some(operator) = words.next() else {
            return false;
        };
        let Some(values) = values.strip_suffix(')') else {
            return false;
        };
        return valid_key(key)
            && matches!(operator, "in" | "notin")
            && words.next().is_none()
            && values.split(',').all(|value| valid_value(value.trim()));
    }
    if let Some(offset) = requirement.find(['=', '!', '>', '<']) {
        let key = requirement[..offset].trim();
        let tail = &requirement[offset..];
        let (operator, value) = if let Some(value) = tail.strip_prefix("!=") {
            ("!=", value)
        } else if let Some(value) = tail.strip_prefix("==") {
            ("==", value)
        } else {
            (&tail[..1], &tail[1..])
        };
        let value = value.trim();
        return valid_key(key)
            && valid_value(value)
            && match operator {
                "=" | "==" | "!=" => true,
                ">" | "<" => value.parse::<i64>().is_ok(),
                _ => false,
            };
    }
    valid_key(requirement)
}

fn valid_key(key: &str) -> bool {
    let name = if let Some((prefix, name)) = key.split_once('/') {
        if prefix.len() > 253
            || !prefix.split('.').all(|label| {
                !label.is_empty()
                    && label.starts_with(|character: char| character.is_ascii_alphanumeric())
                    && label.ends_with(|character: char| character.is_ascii_alphanumeric())
                    && label.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
            })
        {
            return false;
        }
        name
    } else {
        key
    };
    !name.is_empty() && valid_value(name)
}

fn valid_value(value: &str) -> bool {
    value.is_empty()
        || value.len() <= 63
            && value.starts_with(|character: char| character.is_ascii_alphanumeric())
            && value.ends_with(|character: char| character.is_ascii_alphanumeric())
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::valid;

    #[test]
    fn accepts_kubernetes_selector_forms() {
        for selector in [
            "",
            " ",
            "app",
            "!app",
            "! app",
            "app=router",
            "app==router",
            "app!=router",
            "app=",
            "app!=",
            "app in (router, worker)",
            "app notin (router)",
            "app in ()",
            "app in (,router,)",
            "app in(router)",
            "replicas>1",
            "replicas < 10",
            "kinference.buoyant.io/router=edge",
            "App_Name=Value.1",
            "in",
            "notin",
            "app=router,environment in (production,staging),!retired",
        ] {
            assert!(valid(selector), "rejected valid selector {selector:?}");
        }
    }

    #[test]
    fn rejects_invalid_syntax_and_label_names() {
        for selector in [
            "app in (",
            "app in (router))",
            "app in router",
            "app in ((router))",
            "app=router,",
            ",app=router",
            "app=router,,other=value",
            "app router",
            "=router",
            "app===router",
            "app!router",
            "!app=router",
            "app in (a=b)",
            "app>many",
            "app>9223372036854775808",
            "app=-router",
            "-app=router",
            "app/other/name=value",
            "EXAMPLE.com/app=value",
            "/app=value",
            "example.com/=value",
            "app=💡",
            "app\u{a0}=router",
            "app\u{b}=router",
            "app\u{c}=router",
            "app in (router) trailing",
        ] {
            assert!(!valid(selector), "accepted invalid selector {selector:?}");
        }
        assert!(!valid(&format!("{}=value", "a".repeat(64))));
        assert!(!valid(&format!("app={}", "a".repeat(64))));
        assert!(!valid(&format!("{}/app=value", "a".repeat(254))));
    }
}
