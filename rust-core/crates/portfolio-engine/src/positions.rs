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

#[cfg(test)]
mod tests {
    use super::{mark_to_market, update_position, Position};

    fn approx_eq(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "left={} right={}", a, b);
    }

    #[test]
    fn long_then_partial_close_realizes_pnl() {
        let mut pos = Position::default();
        update_position(&mut pos, 10.0, 0.40);
        update_position(&mut pos, -4.0, 0.50);

        approx_eq(pos.qty, 6.0);
        approx_eq(pos.avg_price, 0.40);
        approx_eq(pos.realized_pnl, 0.40);
    }

    #[test]
    fn short_then_cover_realizes_pnl() {
        let mut pos = Position::default();
        update_position(&mut pos, -5.0, 0.60);
        update_position(&mut pos, 2.0, 0.55);

        approx_eq(pos.qty, -3.0);
        approx_eq(pos.avg_price, 0.60);
        approx_eq(pos.realized_pnl, 0.10);
    }

    #[test]
    fn flip_direction_resets_avg_to_flip_fill() {
        let mut pos = Position::default();
        update_position(&mut pos, 3.0, 0.30);
        update_position(&mut pos, -5.0, 0.45);

        approx_eq(pos.qty, -2.0);
        approx_eq(pos.avg_price, 0.45);
        approx_eq(pos.realized_pnl, 0.45);
    }

    #[test]
    fn mark_to_market_includes_realized_and_unrealized() {
        let mut pos = Position::default();
        update_position(&mut pos, 8.0, 0.50);
        update_position(&mut pos, -3.0, 0.60);

        let mtm = mark_to_market(&pos, 0.55);
        approx_eq(mtm, 0.30 + (0.55 - 0.50) * 5.0);
    }
}

