use crate::{Asset, Market};

pub enum MarketType<'a> {
    AsIs(&'a Market),
    Inverted(&'a Market),
}

enum SearchItem {
    Base,
    Quote,
}

pub fn conversion_chain(target: Market, markets: &Vec<Market>) -> Option<Vec<MarketType<'_>>> {
    let mut r = Vec::new();

    let mut last_quote = None;
    loop {
        let asset = last_quote.unwrap_or(&target.base);
        match search_market_for_asset(&asset, SearchItem::Base, markets) {
            Some(mt) => {
                match mt {
                    MarketType::AsIs(m) => last_quote.replace(&m.quote),
                    MarketType::Inverted(m) => last_quote.replace(&m.base)
                };
                r.push(mt);
                // check if we found the last market of the chain
                if last_quote.unwrap() == &target.quote {
                    return Some(r);
                }                
            },
            // couldn't find the next market in the chain
            None => return None,
        }
    }
}

/// Search for a market for the given asset, the asset could be either the base or quote, if it's a quote
/// the market it's returned as inverted, so the final computation of the conversion chain can be done correctly.
fn search_market_for_asset<'a>(
    asset: &Asset,
    search_type: SearchItem,
    markets: &'a Vec<Market>,
) -> Option<MarketType<'a>> {
    let result = match markets.iter().find(|f| f.base == *asset) {
        Some(m) => Some(m),
        None => markets
            .iter()
            .find(|f| f.base == *asset)
            .map(|m| m),
    };

    result.map(|m| match search_type {
        SearchItem::Base => if m.base == *asset { MarketType::AsIs(m) } else { MarketType::Inverted(m) },
        SearchItem::Quote => if m.quote == *asset { MarketType::AsIs(m) } else { MarketType::Inverted(m) },
    })
}
