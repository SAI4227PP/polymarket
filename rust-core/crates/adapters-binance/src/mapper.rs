pub fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace('-', "")
        .replace('_', "")
        .to_lowercase()
}
