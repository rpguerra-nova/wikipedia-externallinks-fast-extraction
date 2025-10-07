extern crate wikipedia_externallinks_fast_extraction;
extern crate rayon;

use std::io;
use wikipedia_externallinks_fast_extraction::iter_string_urls;
use rayon::iter::ParallelIterator;

fn main() {
    let stdin = io::stdin();
    iter_string_urls(stdin.lock()).for_each(|url_result| {
        match url_result {
            Ok((raw_url, path)) => {
                let formatted_url = reformat_url(&raw_url);
                println!("{}{}", formatted_url, path);
            }
            Err(err) => eprintln!("{}", err),
        }
    });
}

fn reformat_url(raw: &str) -> String {
    let parts: Vec<&str> = raw.split("://").collect();
    if parts.len() != 2 {
        return raw.to_string(); // fallback if no scheme
    }

    let scheme = parts[0];
    let domain = parts[1].trim_end_matches('.');

    let domain_parts: Vec<&str> = domain.split('.').collect();
    let reversed_domain = domain_parts.iter().rev().cloned().collect::<Vec<_>>().join(".");

    format!("{}://{}", scheme, reversed_domain)
}
