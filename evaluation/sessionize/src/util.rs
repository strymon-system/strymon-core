use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::fmt::Debug;

pub fn log_discretize(x: u64) -> u64 {
   if x > 0 { ((x as f64).log(2.0) * 100.0).ceil() as u64 } else { u64::max_value() }
}

// Transaction numbers per session     
pub fn convert_trxnb(s: &str) -> Vec<u32> {
    s.split('-').filter_map(|num| {
        if num.is_empty() {
            None  // discard empty segments (e.g. "2-1--8")
        } else {
            Some(::logparse::date_time::parse_positive_decimal(num))
        }
    }).collect()

    // TODO: this is a good use case for SmallVec<[u32; 16]>
    // https://github.com/servo/rust-smallvec
}

fn get_quantiles<V: Debug + Clone + Ord>(quantile_counts: Vec<u64>, all_counts: &Vec<(V, u64)>) -> Vec<V> {
    let mut quantiles : Vec<V> = Vec::new();
    let mut quantile_index : usize = 0;
    let mut cur_sum : Option<u64> = None;
    let mut prev : Option<V> = None;
    // Assuming that the quantile_counts are such that they are (0, total_sum]
    'outer: for count in all_counts.iter() {
       if prev.is_some() { assert!(prev.unwrap().cmp(&count.0) != Ordering::Greater); }
       cur_sum = Some(cur_sum.unwrap_or(0) + count.1);
       while cur_sum.is_some() && cur_sum.unwrap() >= quantile_counts[quantile_index] {
           quantiles.push(count.0.clone());
           quantile_index += 1;
           if quantile_index >= quantile_counts.len() {
               break 'outer;
           }
       }
       prev = Some(count.0.clone());
    }
    assert!(quantile_index == quantile_counts.len(), format!("Failed for {} in {:?} for {:?}", quantile_index, quantile_counts, all_counts));
    quantiles
}

fn discretized_counts<V: Hash + Eq + Clone, F: Fn(V) -> V>(all_counts: Vec<(V, u64)>, discretizer: Option<F>, disable_discretization: bool) -> Vec<(V, u64)> {
    if discretizer.is_none() || disable_discretization {
        all_counts
    } else {
        let discretizer: F = discretizer.unwrap();
        let mut discretized_map : HashMap<V, u64> = HashMap::new();
        for count in all_counts.iter() {
           let discretized_value = discretizer(count.0.clone());
           *discretized_map.entry(discretized_value).or_insert(0) += count.1;
        }
        discretized_map.into_iter().collect()  
    }
}


pub fn to_print_epoch(epoch: u64) -> bool {
    epoch % 60 == 0 || epoch == u64::max_value() / 2
}

pub fn dump_histogram_hash_map<V: Debug + Clone + Eq + Ord + Hash, F: Fn(V) -> V>(prefix: &'static str, worker_index: usize, epoch: u64, mut all_counts: Vec<(V, u64)>, discretizer: Option<F>, disable_discretization: bool) {
    if !to_print_epoch(epoch) || all_counts.len() == 0 {
        return;
    }

    let total_count = all_counts.iter().fold(0, |sum, x| sum + x.1);
    let num_items = all_counts.len();
    let quantile_counts: Vec<u64> = vec![1, 5, 10, 50, 90, 95, 99].iter().map(|x| ((x * total_count) as f64 / 100.0).ceil() as u64).collect::<Vec<_>>();
    all_counts.sort_by(|a, b| a.0.cmp(&b.0));
    let quantiles = get_quantiles(quantile_counts, &all_counts);
    let quantiles_string: Vec<String> = quantiles.iter().map(|x| format!("{:?}", x)).collect();
    let quantiles_string = quantiles_string.join(" ");
    let mut values_dump : Vec<(V, f64)> = discretized_counts(all_counts, discretizer, disable_discretization).iter().map(|x| {
        (x.0.clone(), x.1 as f64) }).filter(|x| x.1 > 0.0).collect();
    values_dump.sort_by(|a, b| a.0.cmp(&b.0));
    let values_dump_vector : Vec<String> = values_dump.iter().map(|x| format!("({:?}: {:.*})", x.0, 2, x.1)).collect();
    let values_dump_str = values_dump_vector.join(" ");
    println!("Worker: {}, Dump Histogram for {}: {:?}: unique_values: {}, total_samples: {}, histogram: {}, quantiles: {}", worker_index, prefix, epoch, num_items, total_count, values_dump_str, quantiles_string);
}
