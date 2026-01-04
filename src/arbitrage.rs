use crate::config::ArbitrageConfig;
use crate::polymarket::{Market, PriceLevel};
use serde::Serialize;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct OutcomeBook {
    pub name: String,
    pub token_id: String,
    pub asks: Vec<PriceLevel>,
    pub bids: Vec<PriceLevel>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum ArbSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize)]
pub struct LegExecution {
    pub name: String,
    pub token_id: String,
    pub avg_price: f64,
    pub limit_price: f64,
    pub unwind_price: f64,
    pub size: f64,
    pub levels_used: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DepthSample {
    pub target_notional: f64,
    pub size: f64,
    pub total_notional: f64,
    pub bundle_price: f64,
    pub bundle_price_after_fees: f64,
    pub edge_bps: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BestCandidate {
    pub size: f64,
    pub total_notional: f64,
    pub bundle_price: f64,
    pub bundle_price_after_fees: f64,
    pub edge_bps: f64,
    pub margin_bps: f64,
}

#[derive(Debug, Clone)]
pub struct MarketEvaluation {
    pub opportunities: Vec<ArbOpportunity>,
    pub best_buy_candidate: Option<BestCandidate>,
    pub depth_samples: Vec<DepthSample>,
}

#[derive(Debug, Clone)]
pub struct ArbOpportunity {
    pub side: ArbSide,
    pub market_id: String,
    pub market_question: String,
    pub market_slug: Option<String>,
    pub size: f64,
    pub probe_size: f64,
    pub probe_max_wait_ms: u64,
    pub bundle_price: f64,
    pub bundle_price_after_fees: f64,
    pub total_notional: f64,
    pub total_after_fees: f64,
    pub profit: f64,
    pub margin: f64,
    pub margin_bps: f64,
    pub fee_bps: f64,
    pub buffer_bps: f64,
    pub required_margin_bps: f64,
    pub depth_samples: Vec<DepthSample>,
    pub legs: Vec<LegExecution>,
}

#[derive(Debug, Clone)]
struct Level {
    price: f64,
    size: f64,
}

#[derive(Debug, Clone)]
struct OutcomeLevels {
    name: String,
    token_id: String,
    levels: Vec<Level>,
    unwind_price: f64,
}


pub fn evaluate_market(
    market: &Market,
    books: &[OutcomeBook],
    config: &ArbitrageConfig,
) -> MarketEvaluation {
    if books.len() < config.min_outcomes || books.len() > config.max_outcomes {
        return MarketEvaluation {
            opportunities: Vec::new(),
            best_buy_candidate: None,
            depth_samples: Vec::new(),
        };
    }

    let buy_eval = evaluate_side_with_diagnostics(market, books, config, ArbSide::Buy);

    let mut opportunities = Vec::new();
    if let Some(opportunity) = buy_eval.best_opportunity {
        opportunities.push(opportunity);
    }

    MarketEvaluation {
        opportunities,
        best_buy_candidate: buy_eval.best_candidate,
        depth_samples: buy_eval.depth_samples,
    }
}

struct SideEvaluation {
    best_opportunity: Option<ArbOpportunity>,
    best_candidate: Option<BestCandidate>,
    depth_samples: Vec<DepthSample>,
}

fn evaluate_side_with_diagnostics(
    market: &Market,
    books: &[OutcomeBook],
    config: &ArbitrageConfig,
    side: ArbSide,
) -> SideEvaluation {
    let mut result = SideEvaluation {
        best_opportunity: None,
        best_candidate: None,
        depth_samples: Vec::new(),
    };

    let outcomes = match prepare_outcomes(books, config, side) {
        Some(outcomes) => outcomes,
        None => return result,
    };
    let max_size = match max_bundle_size(&outcomes) {
        Some(size) => size,
        None => return result,
    };
    if max_size <= 0.0 || max_size < config.min_size {
        return result;
    }

    // Profit is piecewise linear across depth breakpoints; sample those + min_size.
    let sizes = candidate_sizes(
        &outcomes,
        max_size,
        config.min_size,
        &config.target_notionals,
    );
    if sizes.is_empty() {
        return result;
    }

    let fee_multiplier = config.fee_bps / 10_000.0;
    let buffer_margin = config.buffer_bps / 10_000.0;
    let required_margin = config.min_profit + buffer_margin;
    let mut best: Option<ArbOpportunity> = None;
    let mut best_candidate: Option<BestCandidate> = None;
    let mut cost_candidates: Vec<CostCandidate> = Vec::new();

    for size in sizes {
        let mut total_notional = 0.0;
        let mut legs = Vec::with_capacity(outcomes.len());

        let mut valid = true;
        for outcome in &outcomes {
            match walk_levels(&outcome.levels, size) {
                Some((notional, avg_price, levels_used, limit_price)) => {
                    total_notional += notional;
                    legs.push(LegExecution {
                        name: outcome.name.clone(),
                        token_id: outcome.token_id.clone(),
                        avg_price,
                        limit_price,
                        unwind_price: outcome.unwind_price,
                        size,
                        levels_used,
                    });
                }
                None => {
                    valid = false;
                    break;
                }
            }
        }

        if !valid {
            continue;
        }

        let total_after_fees = match side {
            ArbSide::Buy => total_notional * (1.0 + fee_multiplier),
            ArbSide::Sell => total_notional * (1.0 - fee_multiplier),
        };
        let profit = match side {
            ArbSide::Buy => size - total_after_fees,
            ArbSide::Sell => total_after_fees - size,
        };

        let bundle_price = total_notional / size;
        let bundle_price_after_fees = total_after_fees / size;
        let margin = profit / size;
        let edge = match side {
            ArbSide::Buy => 1.0 - bundle_price_after_fees,
            ArbSide::Sell => bundle_price_after_fees - 1.0,
        };
        let edge_bps = edge * 10_000.0;
        let margin_bps = margin * 10_000.0;

        cost_candidates.push(CostCandidate {
            size,
            total_notional,
            bundle_price,
            bundle_price_after_fees,
        });
        let should_replace = match &best_candidate {
            None => true,
            Some(candidate) => edge_bps > candidate.edge_bps + 1e-9,
        };
        if should_replace {
            best_candidate = Some(BestCandidate {
                size,
                total_notional,
                bundle_price,
                bundle_price_after_fees,
                edge_bps,
                margin_bps,
            });
        }

        if profit <= 0.0 {
            continue;
        }

        if margin < required_margin {
            continue;
        }

        if config.min_notional > 0.0 && total_notional < config.min_notional {
            continue;
        }
        if config.max_notional > 0.0 && total_notional > config.max_notional {
            continue;
        }

        let probe_size = compute_probe_size(size, config);
        let opportunity = ArbOpportunity {
            side,
            market_id: market.id.clone(),
            market_question: market.question.clone(),
            market_slug: market.slug.clone(),
            size,
            probe_size,
            probe_max_wait_ms: config.probe_max_wait_ms,
            bundle_price,
            bundle_price_after_fees,
            total_notional,
            total_after_fees,
            profit,
            margin,
            margin_bps: margin * 10_000.0,
            fee_bps: config.fee_bps,
            buffer_bps: config.buffer_bps,
            required_margin_bps: required_margin * 10_000.0,
            depth_samples: Vec::new(),
            legs,
        };

        let replace = match &best {
            None => true,
            Some(current) => {
                if opportunity.profit > current.profit + 1e-9 {
                    true
                } else if (opportunity.profit - current.profit).abs() <= 1e-9 {
                    opportunity.margin > current.margin
                } else {
                    false
                }
            }
        };

        if replace {
            best = Some(opportunity);
        }
    }

    let depth_samples = build_depth_samples(&cost_candidates, &config.target_notionals, side);
    if let Some(mut best) = best {
        best.depth_samples = depth_samples.clone();
        result.best_opportunity = Some(best);
    }
    result.best_candidate = best_candidate;
    result.depth_samples = depth_samples;
    result
}

fn prepare_outcomes(
    books: &[OutcomeBook],
    config: &ArbitrageConfig,
    side: ArbSide,
) -> Option<Vec<OutcomeLevels>> {
    let mut outcomes = Vec::with_capacity(books.len());
    for book in books {
        let raw_levels = match side {
            ArbSide::Buy => &book.asks,
            ArbSide::Sell => &book.bids,
        };
        let levels = sanitize_levels(raw_levels, config.min_price, config.max_price, side);
        if levels.is_empty() {
            return None;
        }
        let unwind_price = match side {
            ArbSide::Buy => best_bid_price(&book.bids)?,
            ArbSide::Sell => best_ask_price(&book.asks)?,
        };
        outcomes.push(OutcomeLevels {
            name: book.name.clone(),
            token_id: book.token_id.clone(),
            levels,
            unwind_price,
        });
    }

    Some(outcomes)
}

fn sanitize_levels(
    levels: &[PriceLevel],
    min_price: f64,
    max_price: f64,
    side: ArbSide,
) -> Vec<Level> {
    let mut filtered: Vec<Level> = levels
        .iter()
        .filter_map(|level| {
            let size = level.size?;
            if size <= 0.0 {
                return None;
            }
            if !level.price.is_finite() || !size.is_finite() {
                return None;
            }
            if level.price < min_price || level.price > max_price {
                return None;
            }
            Some(Level {
                price: level.price,
                size,
            })
        })
        .collect();

    match side {
        ArbSide::Buy => filtered.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(Ordering::Equal)
        }),
        ArbSide::Sell => filtered.sort_by(|a, b| {
            b.price
                .partial_cmp(&a.price)
                .unwrap_or(Ordering::Equal)
        }),
    }

    filtered
}

fn best_bid_price(levels: &[PriceLevel]) -> Option<f64> {
    let mut best: Option<f64> = None;
    for level in levels {
        if !level.price.is_finite() {
            continue;
        }
        if let Some(size) = level.size {
            if !size.is_finite() || size <= 0.0 {
                continue;
            }
        }
        best = match best {
            None => Some(level.price),
            Some(current) => {
                if level.price > current {
                    Some(level.price)
                } else {
                    Some(current)
                }
            }
        };
    }
    best
}

fn best_ask_price(levels: &[PriceLevel]) -> Option<f64> {
    let mut best: Option<f64> = None;
    for level in levels {
        if !level.price.is_finite() {
            continue;
        }
        if let Some(size) = level.size {
            if !size.is_finite() || size <= 0.0 {
                continue;
            }
        }
        best = match best {
            None => Some(level.price),
            Some(current) => {
                if level.price < current {
                    Some(level.price)
                } else {
                    Some(current)
                }
            }
        };
    }
    best
}

fn max_bundle_size(outcomes: &[OutcomeLevels]) -> Option<f64> {
    outcomes
        .iter()
        .map(|outcome| outcome.levels.iter().map(|level| level.size).sum::<f64>())
        .fold(None, |min, size| match min {
            None => Some(size),
            Some(current) => Some(current.min(size)),
        })
}

fn candidate_sizes(
    outcomes: &[OutcomeLevels],
    max_size: f64,
    min_size: f64,
    target_notionals: &[f64],
) -> Vec<f64> {
    let mut sizes = Vec::new();
    for outcome in outcomes {
        let mut cumulative = 0.0;
        for level in &outcome.levels {
            cumulative += level.size;
            sizes.push(cumulative);
        }
    }

    if min_size > 0.0 {
        sizes.push(min_size);
    }
    sizes.push(max_size);

    if !target_notionals.is_empty() {
        let mut best_bundle_price = 0.0;
        for outcome in outcomes {
            if let Some(level) = outcome.levels.first() {
                best_bundle_price += level.price;
            }
        }
        if best_bundle_price > 0.0 {
            for target in target_notionals {
                if *target <= 0.0 {
                    continue;
                }
                sizes.push(target / best_bundle_price);
            }
        }
    }

    sizes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    sizes.dedup_by(|a, b| (*a - *b).abs() <= 1e-9);
    sizes
        .into_iter()
        .filter(|size| *size >= min_size && *size <= max_size + 1e-9)
        .collect()
}

fn walk_levels(levels: &[Level], size: f64) -> Option<(f64, f64, usize, f64)> {
    if size <= 0.0 {
        return None;
    }

    let mut remaining = size;
    let mut notional = 0.0;
    let mut levels_used = 0usize;
    let mut last_price: Option<f64> = None;
    for level in levels {
        if remaining <= 0.0 {
            break;
        }
        let fill = remaining.min(level.size);
        if fill <= 0.0 {
            continue;
        }
        notional += fill * level.price;
        remaining -= fill;
        levels_used += 1;
        last_price = Some(level.price);
    }

    if remaining > 1e-9 {
        return None;
    }

    let avg_price = notional / size;
    let limit_price = last_price?;
    Some((notional, avg_price, levels_used, limit_price))
}

fn compute_probe_size(size: f64, config: &ArbitrageConfig) -> f64 {
    if size <= 0.0 {
        return 0.0;
    }
    let mut probe = size * config.probe_fraction.max(0.0);
    if config.probe_min_size > 0.0 {
        probe = probe.max(config.probe_min_size);
    }
    if probe > size {
        size
    } else {
        probe
    }
}

#[derive(Debug, Clone)]
struct CostCandidate {
    size: f64,
    total_notional: f64,
    bundle_price: f64,
    bundle_price_after_fees: f64,
}

fn build_depth_samples(
    candidates: &[CostCandidate],
    target_notionals: &[f64],
    side: ArbSide,
) -> Vec<DepthSample> {
    if candidates.is_empty() || target_notionals.is_empty() {
        return Vec::new();
    }

    let mut samples = Vec::new();
    for target in target_notionals {
        if *target <= 0.0 {
            continue;
        }
        let mut best: Option<&CostCandidate> = None;
        let mut best_diff = f64::INFINITY;
        for candidate in candidates {
            let diff = (candidate.total_notional - target).abs();
            if diff < best_diff {
                best_diff = diff;
                best = Some(candidate);
            }
        }
        if let Some(candidate) = best {
            let edge = match side {
                ArbSide::Buy => 1.0 - candidate.bundle_price_after_fees,
                ArbSide::Sell => candidate.bundle_price_after_fees - 1.0,
            };
            samples.push(DepthSample {
                target_notional: *target,
                size: candidate.size,
                total_notional: candidate.total_notional,
                bundle_price: candidate.bundle_price,
                bundle_price_after_fees: candidate.bundle_price_after_fees,
                edge_bps: edge * 10_000.0,
            });
        }
    }
    samples
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walk_levels_consumes_depth() {
        let levels = vec![
            Level {
                price: 0.40,
                size: 5.0,
            },
            Level {
                price: 0.50,
                size: 5.0,
            },
        ];
        let (notional, avg_price, levels_used, limit_price) =
            walk_levels(&levels, 6.0).expect("walk levels");
        assert!((notional - 2.5).abs() < 1e-9);
        assert!((avg_price - (2.5 / 6.0)).abs() < 1e-9);
        assert_eq!(levels_used, 2);
        assert!((limit_price - 0.50).abs() < 1e-9);
    }

    #[test]
    fn candidate_sizes_include_targets() {
        let outcomes = vec![
            OutcomeLevels {
                name: "YES".to_string(),
                token_id: "y".to_string(),
                levels: vec![Level {
                    price: 0.40,
                    size: 10.0,
                }],
                unwind_price: 0.30,
            },
            OutcomeLevels {
                name: "NO".to_string(),
                token_id: "n".to_string(),
                levels: vec![Level {
                    price: 0.60,
                    size: 10.0,
                }],
                unwind_price: 0.70,
            },
        ];
        let sizes = candidate_sizes(&outcomes, 10.0, 0.0, &[10.0]);
        assert!(sizes.iter().any(|size| (*size - 10.0).abs() < 1e-9));
    }
}
