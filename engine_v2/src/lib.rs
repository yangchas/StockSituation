use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

const HISTORY_CAPACITY: usize = 250; 
const TOP_N: usize = 20;

#[derive(Clone, Default)]
struct StockExtremes {
    max_price: f64,
    min_price: f64,
    auction_bid_amount: f64,
}

#[derive(Clone, Default)]
struct AuctionSnapshot {
    p0920: f64,
    p0924: f64,
    p0925: f64,
}

#[pyclass]
pub struct MarketEngine {
    symbol_to_index: HashMap<String, usize>,
    index_to_symbol: Vec<String>,
    current_prices: Vec<f64>,
    current_amounts: Vec<f64>,
    open_prices: Vec<f64>,
    extremes: Vec<StockExtremes>,
    auction_snaps: Vec<AuctionSnapshot>,
    price_history: Vec<Vec<f64>>,
    // Advanced: K-line history for Chip and EMA
    daily_k_close: Vec<Vec<f64>>,
    daily_k_vol: Vec<Vec<f64>>,
    daily_k_high: Vec<Vec<f64>>,
    daily_k_low: Vec<Vec<f64>>,
    plate_to_stocks: HashMap<String, Vec<usize>>,
}

#[pymethods]
impl MarketEngine {
    #[new]
    fn new() -> Self {
        MarketEngine {
            symbol_to_index: HashMap::new(),
            index_to_symbol: Vec::new(),
            current_prices: Vec::new(),
            current_amounts: Vec::new(),
            open_prices: Vec::new(),
            extremes: Vec::new(),
            auction_snaps: Vec::new(),
            price_history: Vec::new(),
            daily_k_close: Vec::new(),
            daily_k_vol: Vec::new(),
            daily_k_high: Vec::new(),
            daily_k_low: Vec::new(),
            plate_to_stocks: HashMap::new(),
        }
    }

    fn register_symbols(&mut self, symbols: Vec<String>) {
        for sym in symbols {
            if !self.symbol_to_index.contains_key(&sym) {
                let idx = self.index_to_symbol.len();
                self.symbol_to_index.insert(sym.clone(), idx);
                self.index_to_symbol.push(sym);
                self.current_prices.push(0.0);
                self.current_amounts.push(0.0);
                self.open_prices.push(0.0);
                self.extremes.push(StockExtremes { max_price: 0.0, min_price: 999999.0, auction_bid_amount: 0.0 });
                self.auction_snaps.push(AuctionSnapshot::default());
                self.price_history.push(Vec::with_capacity(60));
                self.daily_k_close.push(Vec::with_capacity(HISTORY_CAPACITY));
                self.daily_k_vol.push(Vec::with_capacity(HISTORY_CAPACITY));
                self.daily_k_high.push(Vec::with_capacity(HISTORY_CAPACITY));
                self.daily_k_low.push(Vec::with_capacity(HISTORY_CAPACITY));
            }
        }
    }

    fn push_tick(&mut self, symbol: &str, price: f64, amount: f64, vol: f64, time_str: &str, bid_amount: f64) {
        if let Some(&idx) = self.symbol_to_index.get(symbol) {
            self.current_prices[idx] = price;
            self.current_amounts[idx] = amount;
            if self.open_prices[idx] == 0.0 && price > 0.0 {
                self.open_prices[idx] = price;
            }
            
            let ext = &mut self.extremes[idx];
            if price > ext.max_price { ext.max_price = price; }
            if price < ext.min_price && price > 0.0 { ext.min_price = price; }
            ext.auction_bid_amount = bid_amount;

            // Auction Snapshots (09:20 - 09:25)
            if time_str >= "09:20:00" && time_str <= "09:20:20" { self.auction_snaps[idx].p0920 = price; }
            if time_str >= "09:24:00" && time_str <= "09:24:20" { self.auction_snaps[idx].p0924 = price; }
            if time_str >= "09:25:00" && time_str <= "09:25:20" { self.auction_snaps[idx].p0925 = price; }

            let hist = &mut self.price_history[idx];
            if hist.len() >= 60 { hist.remove(0); }
            hist.push(price);
        }
    }

    fn register_plate_mapping(&mut self, plate_id: String, symbols: Vec<String>) {
        let indices: Vec<usize> = symbols.iter()
            .filter_map(|s| self.symbol_to_index.get(s))
            .cloned()
            .collect();
        self.plate_to_stocks.insert(plate_id, indices);
    }

    // --- Advanced Logic: Chip & Indicators ---

    fn update_daily_k(&mut self, symbol: &str, close: f64, high: f64, low: f64, vol: f64) {
        if let Some(&idx) = self.symbol_to_index.get(symbol) {
            let limit = HISTORY_CAPACITY;
            let c = &mut self.daily_k_close[idx];
            if c.len() >= limit { c.remove(0); }
            c.push(close);
            let h = &mut self.daily_k_high[idx];
            if h.len() >= limit { h.remove(0); }
            h.push(high);
            let l = &mut self.daily_k_low[idx];
            if l.len() >= limit { l.remove(0); }
            l.push(low);
            let v = &mut self.daily_k_vol[idx];
            if v.len() >= limit { v.remove(0); }
            v.push(vol);
        }
    }

    fn calculate_chip_concentration(&self, symbol: &str, bins: usize) -> PyResult<f64> {
        if let Some(&idx) = self.symbol_to_index.get(symbol) {
            let closes = &self.daily_k_close[idx];
            let highs = &self.daily_k_high[idx];
            let lows = &self.daily_k_low[idx];
            let vols = &self.daily_k_vol[idx];
            
            if closes.is_empty() { return Ok(0.5); }
            
            let mut avg_prices: Vec<f64> = Vec::new();
            for i in 0..closes.len() {
                avg_prices.push((highs[i] + lows[i] + closes[i]) / 3.0);
            }
            
            let min_p = *avg_prices.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0);
            let max_p = *avg_prices.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0);
            if max_p == min_p { return Ok(0.0); }
            
            // Simplified histogram for concentration
            let bin_width = (max_p - min_p) / bins as f64;
            let mut hist = vec![0.0; bins];
            let mut total_vol = 0.0;
            for i in 0..avg_prices.len() {
                let bin_idx = (((avg_prices[i] - min_p) / bin_width).floor() as usize).min(bins - 1);
                hist[bin_idx] += vols[i];
                total_vol += vols[i];
            }
            
            // Concentration: Price range covering top 70% of volume
            let mut indexed_hist: Vec<(usize, f64)> = hist.iter().enumerate().map(|(i, &v)| (i, v)).collect();
            indexed_hist.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            
            let mut acc_vol = 0.0;
            let mut covered_bins = 0;
            for &(_, v) in &indexed_hist {
                acc_vol += v;
                covered_bins += 1;
                if acc_vol >= total_vol * 0.7 { break; }
            }
            
            return Ok(covered_bins as f64 / bins as f64);
        }
        Ok(1.0)
    }

    fn get_snapshot(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        for (idx, sym) in self.index_to_symbol.iter().enumerate() {
            let s_dict = PyDict::new(py);
            s_dict.set_item("price", self.current_prices[idx])?;
            s_dict.set_item("amount", self.current_amounts[idx])?;
            
            let hist = &self.price_history[idx];
            let speed = if hist.len() >= 2 { (hist[hist.len()-1] - hist[0]) / hist[0] } else { 0.0 };
            s_dict.set_item("speed", speed)?;

            let ext = &self.extremes[idx];
            let snap = &self.auction_snaps[idx];
            s_dict.set_item("max_p", ext.max_price)?;
            s_dict.set_item("min_p", ext.min_price)?;
            s_dict.set_item("p0920", snap.p0920)?;
            s_dict.set_item("p0924", snap.p0924)?;
            s_dict.set_item("p0925", snap.p0925)?;
            s_dict.set_item("bid_amt", ext.auction_bid_amount)?;
            
            dict.set_item(sym, s_dict)?;
        }
        
        let mut turnover_vec: Vec<(usize, f64)> = (0..self.index_to_symbol.len()).map(|i| (i, self.current_amounts[i])).collect();
        turnover_vec.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let top_turnover: Vec<String> = turnover_vec.iter().take(TOP_N).map(|&(i, _)| self.index_to_symbol[i].clone()).collect();
        
        let extremes_dict = PyDict::new(py);
        extremes_dict.set_item("top_turnover", top_turnover)?;
        dict.set_item("_EXTREMES_", extremes_dict)?;

        Ok(dict.into())
    }
}

#[pymodule]
fn market_edge_v2_core(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<MarketEngine>()?;
    Ok(())
}
