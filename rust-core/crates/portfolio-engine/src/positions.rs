#[derive(Debug, Default, Clone)]
pub struct Position {
    pub qty: f64,
    pub avg_price: f64,
}

pub fn update_position(pos: &mut Position, qty: f64, price: f64) {
    let new_qty = pos.qty + qty;
    if new_qty.abs() < f64::EPSILON {
        pos.qty = 0.0;
        pos.avg_price = 0.0;
        return;
    }
    pos.avg_price = ((pos.avg_price * pos.qty) + (price * qty)) / new_qty;
    pos.qty = new_qty;
}
