use std::collections::HashSet;

use crate::Market;

/// How this market should be treated when processing the conversion chain.
#[derive(PartialEq, Debug)]
pub enum MarketType<'a> {
    // use the market value as is
    AsIs(&'a Market),
    // invert the market value, e.g. if multiplying, divide 1 by the market value
    Inverted(&'a Market),
}

impl<'a> MarketType<'a> {
    pub fn market(&self) -> &'a Market {
        match self {
            MarketType::AsIs(m) => m,
            MarketType::Inverted(m) => m,
        }
    }
}

/// Store the state of the current path, with this data we can resume searching
/// on a different path by fetching the next element from the filtered iterator.
/// The seen set will prevent cycles when searching in case we already saw a 
/// given market when creating the conversion chain.
#[derive(Debug)]
struct State<'a, T> {
    seen: HashSet<&'a Market>,
    filtered: T,
}

/// A simple backtracking search to find the conversion chain for the given market.
/// It will search for an initial market that has the target's base asset as base,
/// then it will add markets to the chain to cancel terms until it finds a market that
/// has the target's quote. 
/// This is like solving a routing problem where we want to find the path to go from 
/// A to B and the nodes can only be connected if they share an attribute's value.
/// In other words, this performs unit conversions where the base asset is the enumerator
/// unit and the quote asset is the denominator unit.
pub fn conversion_chain<'a: 'b, 'b>(
    target: Market,
    markets: &'a Vec<Market>,
) -> Option<Vec<MarketType<'_>>> {
    // returns an iterator that will keep track of the last market that was used to search
    // for more markets to create the conversion chain.
    let filter_markets = |asset: &'b String| {
        markets
            .iter()
            .filter_map(move |m| -> Option<MarketType<'a>> {
                if m.base == *asset {
                    Some(MarketType::AsIs(m))
                } else if m.quote == *asset {
                    Some(MarketType::Inverted(m))
                } else {
                    None
                }
            })
            .into_iter()
    };

    // keep track of each state when adding markets to the chain (path), with this it can
    // resume the search on a different path if a dead end is reached.
    let mut states = Vec::new();
    // initial state
    let mut curr_state = State {
        seen: HashSet::new(),
        filtered: filter_markets(&target.base),
    };
    let mut chain = Vec::new();

    loop {
        match curr_state.filtered.next() {
            Some(next) => {
                if curr_state.seen.contains(next.market()) {
                    continue;
                }
                // mark this market as seen, so we don't cycle if we see it again
                curr_state.seen.insert(next.market());

                let asset = match next {
                    MarketType::AsIs(m) => &m.quote,
                    MarketType::Inverted(m) => &m.base,
                };

                chain.push(next);
                // found the last market of the chain
                if *asset == target.quote {
                    return Some(chain);
                }

                // clone the markets seen up to this point, this will allow each search
                // path to have it's own seen set.
                let seen = curr_state.seen.clone();
                // keep track of the last state before going into the next iteration, we can
                // pop if we need to continue searching on a different path. Each state will
                // have a separate `filtered` iterator that will keep track of which market
                // is the next one to try for a new search path.
                states.push(curr_state);
                // the new state to continue searching with
                curr_state = State {
                    seen,
                    filtered: filter_markets(asset),
                };
            }
            None => {
                // backtrack, take the last state off the stack and continue searching there
                // from the `filtered` iterator's next element.
                match states.pop() {
                    Some(state) => {
                        curr_state = state;
                        // remove the latest added item as it's not valid anymore
                        chain.pop();
                    }
                    // no more states to search on
                    None => return None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! m {
        ($base:ident-$quote:ident) => {
            Market::new(stringify!($base), stringify!($quote))
        };
    }

    macro_rules! asis {
        ($base:ident-$quote:ident) => {
            MarketType::AsIs(&m!($base - $quote))
        };
    }

    macro_rules! inv {
        ($base:ident-$quote:ident) => {
            MarketType::Inverted(&m!($base - $quote))
        };
    }

    #[test]
    fn test_conversion_chain() {
        let markets = vec![
            m!(BTC - USD),
            m!(BTC - EUR),
            m!(UNI - ETH),
            m!(ETH - USD),
            m!(ETH - BTC),
            m!(ADA - BTC),
        ];

        assert_eq!(
            conversion_chain(m!(ADA - ETH), &markets),
            Some(vec![asis!(ADA - BTC), asis!(BTC - USD), inv!(ETH - USD)])
        );

        assert_eq!(
            conversion_chain(m!(ADA - USD), &markets),
            Some(vec![asis!(ADA - BTC), asis!(BTC - USD)])
        );

        assert_eq!(
            conversion_chain(m!(ADA - EUR), &markets),
            Some(vec![
                asis!(ADA - BTC),
                asis!(BTC - USD),
                inv!(ETH - USD),
                asis!(ETH - BTC),
                asis!(BTC - EUR),
            ])
        );

        assert_eq!(
            conversion_chain(m!(UNI - USD), &markets),
            Some(vec![asis!(UNI - ETH), asis!(ETH - USD)])
        );

        assert_eq!(
            conversion_chain(m!(UNI - BTC), &markets),
            Some(vec![asis!(UNI - ETH), asis!(ETH - USD), inv!(BTC - USD)])
        );

        assert_eq!(
            conversion_chain(m!(UNI - ADA), &markets),
            Some(vec![
                asis!(UNI - ETH),
                asis!(ETH - USD),
                inv!(BTC - USD),
                inv!(ADA - BTC)
            ])
        );

        assert_eq!(conversion_chain(m!(ADA - DOT), &markets), None);

        assert_eq!(conversion_chain(m!(DOT - ADA), &markets), None);
    }

    #[test]
    fn test_search_market_for_asset() {}
}
