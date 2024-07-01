use rust_public_transit_sim::StaticSimulator;
use env_logger;
use std::io::prelude::*;


fn parse_routes_txt(routes_path: &str) -> Vec<Vec<usize>> {
    // open the file
    let path = std::path::Path::new(routes_path);
    let file = match std::fs::File::open(&path) {
        Err(why) => panic!("couldn't open {}: {}", path.display(), why),
        Ok(file) => file,
    };

    // for each line, parse it into a vector by splitting on commas
    let reader = std::io::BufReader::new(file);
    let mut routes = vec![];
    for line in reader.lines() {
        if let Ok(line) = line {
            let split = line.split(',');
            let route: Vec<usize> = split.map(|ss| ss.parse::<usize>().unwrap()).collect();
            routes.push(route);
        }
    }

    return routes;
}

fn main () {
    env_logger::init();
    let mut sim = StaticSimulator::from_cfg("/home/andrew/transit_learning/simulation/laval_cfg.yaml");
    let routes = parse_routes_txt("/home/andrew/transit_learning/routes.txt");
    let freqs = vec![6. / 3600.; routes.len()];
    // let result = sim.run(&routes, &freqs, &vehicle_types, false);
    // let result = sim.run(&vec![vec![50, 21], vec![50, 26], vec![50, 92]], 
    //                      &vec![0.0033333, 0.003333, 0.003333]);
    // println!("satisfied demand: {:?}", result.satisfied_demand);
}
