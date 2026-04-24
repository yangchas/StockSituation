use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

const PRICE_HISTORY_CAPACITY: usize = 60;
const TOP_N: usize = 20;
const MINUTE_HISTORY_KEEP: usize = 12;
const INVALID_MINUTE: i32 = i32::MIN;
const PRICE_SCALE: i64 = 1_000;
const AMOUNT_WAN_SCALE: i64 = 10_000;
const VOLUME_SCALE: i64 = 1;

#[derive(Clone, Default)]
struct StockExtremes {
    max_price_milli: i64,
    min_price_milli: i64,
    auction_bid_amount: i64,
}

#[derive(Clone, Default)]
struct AuctionSnapshot {
    p0920_milli: i64,
    p0924_milli: i64,
    p0925_milli: i64,
}

#[derive(Clone, Copy, Default)]
struct MinuteBar {
    price_milli: i64,
    amount_cum: i64,
}

#[derive(Clone, Copy)]
struct MinuteSlot {
    minute: i32,
    bar: MinuteBar,
}

impl Default for MinuteSlot {
    fn default() -> Self {
        Self {
            minute: INVALID_MINUTE,
            bar: MinuteBar::default(),
        }
    }
}

#[derive(Clone, Copy)]
struct PriceRing {
    values: [i64; PRICE_HISTORY_CAPACITY],
    len: usize,
    write_index: usize,
}

impl Default for PriceRing {
    fn default() -> Self {
        Self {
            values: [0; PRICE_HISTORY_CAPACITY],
            len: 0,
            write_index: 0,
        }
    }
}

impl PriceRing {
    fn push(&mut self, price_milli: i64) {
        self.values[self.write_index] = price_milli;
        self.write_index = (self.write_index + 1) % PRICE_HISTORY_CAPACITY;
        if self.len < PRICE_HISTORY_CAPACITY {
            self.len += 1;
        }
    }

    fn oldest(&self) -> Option<i64> {
        if self.len == 0 {
            return None;
        }
        let index = if self.len < PRICE_HISTORY_CAPACITY {
            0
        } else {
            self.write_index
        };
        Some(self.values[index])
    }

    fn newest(&self) -> Option<i64> {
        if self.len == 0 {
            return None;
        }
        let index = if self.write_index == 0 {
            PRICE_HISTORY_CAPACITY - 1
        } else {
            self.write_index - 1
        };
        Some(self.values[index])
    }

    fn speed_full_window(&self) -> f64 {
        match (self.oldest(), self.newest()) {
            (Some(first), Some(last)) if first > 0 => (last - first) as f64 / first as f64,
            _ => 0.0,
        }
    }
}

#[pyclass]
pub struct MarketEngine {
    symbol_to_index: HashMap<String, usize>,
    index_to_symbol: Vec<String>,
    current_prices: Vec<i64>,
    current_amounts: Vec<i64>,
    current_volumes: Vec<i64>,
    extremes: Vec<StockExtremes>,
    auction_snaps: Vec<AuctionSnapshot>,
    price_history: Vec<PriceRing>,
    minute_rings: Vec<[MinuteSlot; MINUTE_HISTORY_KEEP]>,
    latest_minutes: Vec<i32>,
    plate_to_stocks: HashMap<String, Vec<usize>>,
}

#[pymethods]
impl MarketEngine {
    #[new]
    fn new() -> Self {
        Self {
            symbol_to_index: HashMap::new(),
            index_to_symbol: Vec::new(),
            current_prices: Vec::new(),
            current_amounts: Vec::new(),
            current_volumes: Vec::new(),
            extremes: Vec::new(),
            auction_snaps: Vec::new(),
            price_history: Vec::new(),
            minute_rings: Vec::new(),
            latest_minutes: Vec::new(),
            plate_to_stocks: HashMap::new(),
        }
    }

    fn register_symbols(&mut self, symbols: Vec<String>) {
        for symbol in symbols {
            if self.symbol_to_index.contains_key(&symbol) {
                continue;
            }
            let idx = self.index_to_symbol.len();
            self.symbol_to_index.insert(symbol.clone(), idx);
            self.index_to_symbol.push(symbol);
            self.current_prices.push(0);
            self.current_amounts.push(0);
            self.current_volumes.push(0);
            self.extremes.push(StockExtremes {
                max_price_milli: 0,
                min_price_milli: i64::MAX,
                auction_bid_amount: 0,
            });
            self.auction_snaps.push(AuctionSnapshot::default());
            self.price_history.push(PriceRing::default());
            self.minute_rings.push([MinuteSlot::default(); MINUTE_HISTORY_KEEP]);
            self.latest_minutes.push(INVALID_MINUTE);
        }
    }

    fn register_plate_mapping(&mut self, plate_id: String, symbols: Vec<String>) {
        let indices: Vec<usize> = symbols
            .iter()
            .filter_map(|symbol| self.symbol_to_index.get(symbol))
            .copied()
            .collect();
        self.plate_to_stocks.insert(plate_id, indices);
    }

    #[pyo3(signature = (symbol, price, amount, volume, time_str="00:00:00", bid_amount=0.0))]
    fn push_tick(
        &mut self,
        symbol: &str,
        price: f64,
        amount: f64,
        volume: f64,
        time_str: &str,
        bid_amount: f64,
    ) {
        if let Some(&idx) = self.symbol_to_index.get(symbol) {
            let price_milli = quantize_price(price);
            let amount_i64 = quantize_amount(amount);
            let volume_i64 = quantize_volume(volume);
            let bid_amount_i64 = quantize_amount(bid_amount);

            self.current_prices[idx] = price_milli;
            self.current_amounts[idx] = amount_i64;
            self.current_volumes[idx] = volume_i64;

            let ext = &mut self.extremes[idx];
            if price_milli > ext.max_price_milli {
                ext.max_price_milli = price_milli;
            }
            if price_milli > 0 && price_milli < ext.min_price_milli {
                ext.min_price_milli = price_milli;
            }
            ext.auction_bid_amount = bid_amount_i64;

            let auction = &mut self.auction_snaps[idx];
            if time_str >= "09:20:00" && time_str <= "09:20:20" {
                auction.p0920_milli = price_milli;
            }
            if time_str >= "09:24:00" && time_str <= "09:24:20" {
                auction.p0924_milli = price_milli;
            }
            if time_str >= "09:25:00" && time_str <= "09:25:20" {
                auction.p0925_milli = price_milli;
            }

            self.price_history[idx].push(price_milli);

            let minute_index = parse_minute_index(time_str);
            update_minute_ring(
                &mut self.minute_rings[idx],
                &mut self.latest_minutes[idx],
                minute_index,
                price_milli,
                amount_i64.max(0),
            );
        }
    }

    fn get_snapshot(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);

        for (idx, symbol) in self.index_to_symbol.iter().enumerate() {
            let stock = PyDict::new(py);
            let speed = self.price_history[idx].speed_full_window();
            let ext = &self.extremes[idx];
            let snap = &self.auction_snaps[idx];
            let latest_minute = self.latest_minutes[idx];
            let (speed_1m, amount_2m, amount_5m, vector_3m, vector_5m) = if latest_minute != INVALID_MINUTE {
                compute_minute_metrics(&self.minute_rings[idx], latest_minute)
            } else {
                (speed, 0, 0, 0.0, 0.0)
            };

            stock.set_item("price_milli", self.current_prices[idx])?;
            stock.set_item("amount_wan", to_wan(self.current_amounts[idx]))?;
            stock.set_item("volume", self.current_volumes[idx])?;
            stock.set_item("speed_bp", to_bp(speed_1m))?;
            stock.set_item("amount_2m_wan", to_wan_i64(amount_2m))?;
            stock.set_item("amount_5m_wan", to_wan_i64(amount_5m))?;
            stock.set_item("vector_3m_bp", to_bp(vector_3m))?;
            stock.set_item("vector_5m_bp", to_bp(vector_5m))?;
            stock.set_item("bid_amt_wan", to_wan(ext.auction_bid_amount))?;
            stock.set_item("max_p_milli", ext.max_price_milli)?;            
            stock.set_item(
                "min_p_milli",
                if ext.min_price_milli == i64::MAX { 0 } else { ext.min_price_milli },
            )?;
            stock.set_item("p0920_milli", snap.p0920_milli)?;
            stock.set_item("p0924_milli", snap.p0924_milli)?;
            stock.set_item("p0925_milli", snap.p0925_milli)?;

            dict.set_item(symbol, stock)?;
        }

        let mut turnover_vec: Vec<(usize, i64)> = (0..self.index_to_symbol.len())
            .map(|idx| (idx, self.current_amounts[idx]))
            .collect();
        turnover_vec.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let top_turnover: Vec<String> = turnover_vec
            .iter()
            .take(TOP_N)
            .map(|(idx, _)| self.index_to_symbol[*idx].clone())
            .collect();

        let extremes = PyDict::new(py);
        extremes.set_item("top_turnover", top_turnover)?;
        dict.set_item("_EXTREMES_", extremes)?;

        Ok(dict.into())
    }
}

#[pymodule]
fn engine_next_core(_py: Python, module: &PyModule) -> PyResult<()> {
    module.add_class::<MarketEngine>()?;
    Ok(())
}

fn parse_minute_index(time_str: &str) -> i32 {
    let mut parts = time_str.split(':');
    let hour = parts.next().and_then(|value| value.parse::<i32>().ok()).unwrap_or(0);
    let minute = parts.next().and_then(|value| value.parse::<i32>().ok()).unwrap_or(0);
    hour * 60 + minute
}

fn minute_ring_index(minute: i32) -> usize {
    minute.rem_euclid(MINUTE_HISTORY_KEEP as i32) as usize
}

fn get_minute_bar(ring: &[MinuteSlot; MINUTE_HISTORY_KEEP], minute: i32) -> Option<MinuteBar> {
    let slot = ring[minute_ring_index(minute)];
    if slot.minute == minute {
        Some(slot.bar)
    } else {
        None
    }
}

fn update_minute_ring(
    ring: &mut [MinuteSlot; MINUTE_HISTORY_KEEP],
    latest_minute: &mut i32,
    minute: i32,
    price_milli: i64,
    amount: i64,
) {
    if *latest_minute != INVALID_MINUTE && minute < *latest_minute - MINUTE_HISTORY_KEEP as i32 {
        return;
    }
    if *latest_minute == INVALID_MINUTE || minute > *latest_minute {
        *latest_minute = minute;
    }

    let index = minute_ring_index(minute);
    let slot = &mut ring[index];
    if slot.minute != minute {
        *slot = MinuteSlot {
            minute,
            bar: MinuteBar {
                price_milli,
                amount_cum: amount,
            },
        };
        return;
    }

    slot.bar.price_milli = price_milli;
    if amount >= slot.bar.amount_cum {
        slot.bar.amount_cum = amount;
    }
}

fn compute_minute_metrics(ring: &[MinuteSlot; MINUTE_HISTORY_KEEP], minute: i32) -> (f64, i64, i64, f64, f64) {
    let m0 = match get_minute_bar(ring, minute) {
        Some(value) => value,
        None => return (0.0, 0, 0, 0.0, 0.0),
    };
    let m1 = get_minute_bar(ring, minute - 1);

    let speed_1m = match m1 {
        Some(prev) if prev.price_milli > 0 => (m0.price_milli - prev.price_milli) as f64 / prev.price_milli as f64,
        _ => 0.0,
    };
    let amount_2m = rolling_amount(ring, minute, 2);
    let amount_5m = rolling_amount(ring, minute, 5);
    let vector_3m = rolling_vector(ring, minute, 3);
    let vector_5m = rolling_vector(ring, minute, 5);
    (speed_1m, amount_2m, amount_5m, vector_3m, vector_5m)
}

fn rolling_amount(ring: &[MinuteSlot; MINUTE_HISTORY_KEEP], minute: i32, window: usize) -> i64 {
    if window == 0 {
        return 0;
    }
    let current = match get_minute_bar(ring, minute) {
        Some(bar) => bar.amount_cum,
        None => return 0,
    };
    let mut reference = None;
    for offset in (1..=window).rev() {
        if let Some(bar) = get_minute_bar(ring, minute - offset as i32) {
            reference = Some(bar.amount_cum);
            break;
        }
    }
    match reference {
        Some(amount) if current >= amount => current - amount,
        Some(_) => 0,
        None => 0,
    }
}

fn rolling_vector(ring: &[MinuteSlot; MINUTE_HISTORY_KEEP], minute: i32, window: usize) -> f64 {
    if window == 0 {
        return 0.0;
    }
    let current = match get_minute_bar(ring, minute) {
        Some(bar) if bar.price_milli > 0 => bar.price_milli,
        _ => return 0.0,
    };
    let reference_minute = minute - (window as i32 - 1);
    let reference = match get_minute_bar(ring, reference_minute) {
        Some(bar) if bar.price_milli > 0 => bar.price_milli,
        _ => return 0.0,
    };
    (current - reference) as f64 / reference as f64
}

fn quantize_price(value: f64) -> i64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    (value * PRICE_SCALE as f64).round() as i64
}

fn dequantize_price(value: i64) -> f64 {
    if value <= 0 {
        return 0.0;
    }
    value as f64 / PRICE_SCALE as f64
}

fn quantize_amount(value: f64) -> i64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    value.round() as i64
}

fn quantize_volume(value: f64) -> i64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    (value * VOLUME_SCALE as f64).round() as i64
}

fn dequantize_volume(value: i64) -> f64 {
    if value <= 0 {
        return 0.0;
    }
    value as f64 / VOLUME_SCALE as f64
}

fn to_wan(value: i64) -> i64 {
    if value <= 0 {
        return 0;
    }
    value / AMOUNT_WAN_SCALE
}

fn to_wan_i64(value: i64) -> i64 {
    if value <= 0 {
        return 0;
    }
    value / AMOUNT_WAN_SCALE
}

fn to_bp(value: f64) -> i64 {
    if !value.is_finite() {
        return 0;
    }
    (value * 10_000.0).round() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_minute_ring_keeps_latest_cumulative_amount_per_minute() {
        let mut ring = [MinuteSlot::default(); MINUTE_HISTORY_KEEP];
        let mut latest_minute = INVALID_MINUTE;

        update_minute_ring(&mut ring, &mut latest_minute, 571, 10_000, 100_000_000);
        update_minute_ring(&mut ring, &mut latest_minute, 571, 10_100, 120_000_000);
        update_minute_ring(&mut ring, &mut latest_minute, 571, 10_200, 110_000_000);

        let bar = get_minute_bar(&ring, 571).unwrap();
        assert_eq!(bar.price_milli, 10_200);
        assert_eq!(bar.amount_cum, 120_000_000);
    }

    #[test]
    fn rolling_amount_uses_cumulative_difference_instead_of_summing_minutes() {
        let mut ring = [MinuteSlot::default(); MINUTE_HISTORY_KEEP];
        let mut latest_minute = INVALID_MINUTE;

        update_minute_ring(&mut ring, &mut latest_minute, 570, 10_000, 100_000_000);
        update_minute_ring(&mut ring, &mut latest_minute, 571, 10_200, 130_000_000);
        update_minute_ring(&mut ring, &mut latest_minute, 572, 10_300, 160_000_000);

        assert_eq!(rolling_amount(&ring, 572, 2), 60_000_000);
        assert_eq!(rolling_amount(&ring, 572, 5), 60_000_000);
    }
}
