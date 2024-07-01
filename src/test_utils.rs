use std::collections::HashMap;
use std::fmt::Debug;


/// Checks that the contents of two hashmaps are the same.
pub fn compare_hashmaps<KK, VV>(query_map: &HashMap<KK, VV>, true_map: &HashMap<KK, VV>)
    where KK: Debug + Eq + std::hash::Hash,
    VV: Debug + PartialEq,
{
        assert_eq!(query_map.len(), true_map.len());

        // Check that all the stop facilities match
        for (true_key, true_val) in true_map {
        match query_map.get(true_key) {
        Some(val) => assert_eq!(val, true_val),
        None => assert!(false, "Key {:?} missing!", true_key),
        }
    }
}
