use std::cmp::Ordering;

pub fn cmp_asc(a: f64, b: f64) -> Ordering {
    a.total_cmp(&b)
}

pub fn cmp_desc(a: f64, b: f64) -> Ordering {
    b.total_cmp(&a)
}

pub fn cmp_price(a: f64, b: f64, is_ask: bool) -> Ordering {
    if is_ask {
        cmp_asc(a, b)
    } else {
        cmp_desc(a, b)
    }
}
