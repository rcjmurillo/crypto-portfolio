use crate::Market;

pub enum MarketType<'a> {
    AsIs(&'a Market),
    Inverted(&'a Market),
}

pub fn conversion_chain(target: Market, markets: &Vec<Market>) -> Vec<MarketType<'_>> {
    let mut r = Vec::new();

    let mut last_quote = None;
    loop {
        match last_quote {
            None => match markets.iter().find(|m| m.base == target.base) {
                Some(m) => {
                    r.push(MarketType::AsIs(m));
                    last_quote.replace(&m.quote);
                }
                None => break,
            },
            Some(a) => {
                // search for a market with the target quote asset as quote
                match markets
                    .iter()
                    .find(|m| m.base == *a && m.quote == target.quote)
                {
                    Some(m) => {
                        r.push(MarketType::AsIs(m));
                        // found the last market to convert to the final value
                        break;
                    }
                    // search for a market with the last quote as base only
                    None => match markets.iter().find(|m| m.base == *a) {
                        Some(m) => {
                            r.push(MarketType::AsIs(m));
                            last_quote.replace(&m.quote);
                        }
                        // TODO: search for the inverted market m.quote == a then invert the market, 
                        // last_quote should be the base.
                        None => todo!(),
                    },
                }
            }
        }
    }
    r
}
