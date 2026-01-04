use crate::config::MarketSelectionConfig;
use crate::polymarket::Market;

#[derive(Debug, Clone)]
pub struct MarketScore {
    pub score: f64,
    pub reasons: Vec<String>,
}

pub fn score_market(market: &Market, config: &MarketSelectionConfig) -> MarketScore {
    let mut score = 0.0;
    let mut reasons = Vec::new();

    let question = market.question.to_lowercase();

    if contains_any(&question, &config.boring_keywords) {
        score += config.bonus_boring;
        reasons.push("boring_keyword".to_string());
    }
    if contains_any(&question, &config.breaking_keywords) {
        score -= config.penalty_breaking;
        reasons.push("breaking_keyword".to_string());
    }
    if contains_any(&question, &config.legal_keywords) {
        score -= config.penalty_legal;
        reasons.push("legal_keyword".to_string());
    }
    if contains_any(&question, &config.election_keywords) {
        score -= config.penalty_election;
        reasons.push("election_keyword".to_string());
    }
    if contains_any(&question, &config.niche_keywords) {
        score -= config.penalty_niche;
        reasons.push("niche_keyword".to_string());
    }

    if config.min_volume > 0.0 {
        if let Some(volume) = market.volume {
            if volume >= config.min_volume {
                score += config.bonus_volume;
                reasons.push("high_volume".to_string());
            } else if volume > 0.0 {
                score -= config.penalty_niche;
                reasons.push("low_volume".to_string());
            }
        }
    }

    if config.min_volume_24h > 0.0 {
        if let Some(volume_24h) = market.volume_24h {
            if volume_24h >= config.min_volume_24h {
                score += config.bonus_volume_24h;
                reasons.push("high_volume_24h".to_string());
            } else if volume_24h > 0.0 {
                score -= config.penalty_niche * 0.5;
                reasons.push("low_volume_24h".to_string());
            }
        }
    }

    if config.min_liquidity > 0.0 {
        if let Some(liquidity) = market.liquidity {
            if liquidity >= config.min_liquidity {
                score += config.bonus_liquidity;
                reasons.push("high_liquidity".to_string());
            } else if liquidity > 0.0 {
                score -= config.penalty_niche * 0.5;
                reasons.push("low_liquidity".to_string());
            }
        }
    }

    if config.min_traders > 0 {
        if let Some(traders) = market.num_traders.or(market.num_trades) {
            if traders >= config.min_traders as u64 {
                score += config.bonus_retail;
                reasons.push("many_traders".to_string());
            } else if traders > 0 {
                score -= config.penalty_niche * 0.5;
                reasons.push("few_traders".to_string());
            }
        }
    }

    if near_consensus(&market.outcome_prices, config.consensus_threshold) {
        score += config.bonus_consensus;
        reasons.push("near_consensus".to_string());
    }

    MarketScore { score, reasons }
}

fn near_consensus(prices: &[f64], threshold: f64) -> bool {
    if prices.len() != 2 {
        return false;
    }
    if threshold <= 0.0 || threshold >= 0.5 {
        return false;
    }
    let mut min_price = f64::INFINITY;
    let mut max_price = f64::NEG_INFINITY;
    for price in prices {
        if !price.is_finite() {
            return false;
        }
        if *price < min_price {
            min_price = *price;
        }
        if *price > max_price {
            max_price = *price;
        }
    }
    min_price <= threshold && max_price >= 1.0 - threshold
}

fn contains_any(text: &str, keywords: &[String]) -> bool {
    if keywords.is_empty() {
        return false;
    }
    for keyword in keywords {
        let needle = keyword.trim().to_lowercase();
        if needle.is_empty() {
            continue;
        }
        if text.contains(&needle) {
            return true;
        }
    }
    false
}
