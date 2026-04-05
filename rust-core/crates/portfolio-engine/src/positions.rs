#[derive(Debug, Default, Clone)]
pub struct Position {
    pub qty: f64,
    pub avg_price: f64,
    pub realized_pnl: f64,
}

pub fn update_position(pos: &mut Position, fill_qty: f64, fill_price: f64) {
    if fill_qty == 0.0 {
        return;
    }

    let existing_qty = pos.qty;

    if existing_qty == 0.0 || existing_qty.signum() == fill_qty.signum() {
        let new_qty = existing_qty + fill_qty;
        pos.avg_price = if new_qty.abs() < f64::EPSILON {
            0.0
        } else {
            ((pos.avg_price * existing_qty.abs()) + (fill_price * fill_qty.abs())) / new_qty.abs()
        };
        pos.qty = new_qty;
        return;
    }

    let closing_qty = existing_qty.abs().min(fill_qty.abs());
    let pnl_per_unit = if existing_qty > 0.0 {
        fill_price - pos.avg_price
    } else {
        pos.avg_price - fill_price
    };
    pos.realized_pnl += pnl_per_unit * closing_qty;

    let new_qty = existing_qty + fill_qty;
    if new_qty.abs() < f64::EPSILON {
        pos.qty = 0.0;
        pos.avg_price = 0.0;
    } else {
        pos.qty = new_qty;
        if existing_qty.abs() < fill_qty.abs() {
            pos.avg_price = fill_price;
        }
    }
}

pub fn mark_to_market(pos: &Position, mark_price: f64) -> f64 {
    pos.realized_pnl + (mark_price - pos.avg_price) * pos.qty
}
