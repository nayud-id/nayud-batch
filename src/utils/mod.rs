pub fn now_millis() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

pub fn mask_secret<S: AsRef<str>>(input: S) -> String {
    let s = input.as_ref();
    let len = s.chars().count();
    if len <= 4 { return "****".to_string(); }
    let start: String = s.chars().take(2).collect();
    let end: String = s.chars().rev().take(2).collect::<String>().chars().rev().collect();
    format!("{}****{}", start, end)
}