use std::path::PathBuf;
use std::fs::File;
use std::collections::HashMap;
use std::collections::HashSet;
use glob::glob;

use rust_public_transit_sim::DynamicSimulator;
use rust_public_transit_sim::PtSystem;
use rust_public_transit_sim::JourneyLeg;
use rust_public_transit_sim::VehicleState;
use rust_public_transit_sim::PtVehicle;
use rust_public_transit_sim::PassengerTrip;


#[test]
fn test_assign_trips_general() {
    let root_dir = PathBuf::from("tests/envs");
    for env_dir in &["pt-simple", "pt-multilines"] {
        println!("Testing {:?}", env_dir);
        let mut root_dir = root_dir.clone();
        root_dir.push(env_dir);
        test_assign_trips_on_env(&root_dir);
    }
}

/// Tests that walking legs are properly added to planned journeys.
/// this allows logging errors from tests.
#[test]
fn test_assign_trips_walklegs() {
    let env_dir = PathBuf::from("tests/envs/walking-transfers");

    let mut env_cfg_path = env_dir.clone();
    env_cfg_path.push("config.xml");

    // create the simulator
    // remove the limit on transfers, since we're explicitly creating lots of them in this test
    let sim = DynamicSimulator::new("tests/envs/walking-transfers/config.yaml").unwrap();
    assert!(sim.get_active_trips().len() > 0);

    let mut trip_walk_leg_counts = HashMap::new();
    let file = File::open(sim.get_od_path()).unwrap();
    let mut reader = csv::Reader::from_reader(file);

    for result in reader.deserialize() {
        let row: HashMap<String, String> = result.unwrap();
        let id = row.get("ID").unwrap().clone();
        let walk_leg_count: u32 = row.get("num_walks").unwrap().parse().unwrap();
        trip_walk_leg_counts.insert(id, walk_leg_count);
    }

    for trip in sim.get_active_trips() {
        let true_walk_leg_count = *trip_walk_leg_counts.get(&trip.id).unwrap();
        let walk_leg_count = trip.get_planned_journey().len() - trip.get_planned_ptlegs().len();
        assert_eq!(true_walk_leg_count as usize, walk_leg_count);
    }
}

/// Tests that reducing the max_transfers value reduces the allowed
/// trip length.

/// The environment contains four possible transit routes from the origin to
/// the destination, requiring 1, 2, and 3 transfers.  Unrealistically, the
/// more transfers a route has, the less time is the overall journey.  So
/// without a limit, the journey with 3 transfers will be taken; with a limit
/// of 3, the 2-transfer journey will be taken; etc.
#[test]
fn test_assign_trips_transferlimit() {
    // walk very slowly, so the trip doesn't wind up walking instead of taking the
    // intended route.    
    for path in glob("tests/envs/switch-routes/config_maxtransfers_*.yaml").
                expect("Failed to read glob pattern") {
        let path = path.unwrap();
        let sim = DynamicSimulator::new(path.to_str().unwrap()).unwrap();
        assert_eq!(sim.get_active_trips().len(), 1);
        let trip = &sim.get_active_trips()[0];
        let pjlen = trip.get_planned_ptlegs().len();
        let num_transfers = std::cmp::max(pjlen as i16 - 1, 0) as u8;
        // test that there are two walking legs, one at the start and one at the end.
        assert_eq!(pjlen + 2, trip.get_planned_journey().len());
        assert_eq!(num_transfers, sim.get_max_transfers().unwrap());
    }
}

fn test_assign_trips_on_env(env_dir: &PathBuf) {
    let mut env_cfg_path = env_dir.clone();
    env_cfg_path.push("config.yaml");
    let sim = DynamicSimulator::new(env_cfg_path.to_str().unwrap()).unwrap();
    // check that all trips in sim.tracked_trips have the correct dep and arr
    // check that all trips with a dep and arr in oddf are in sim.tracked_trips

    let mut tracked_trip_ids = vec![];
    let mut tracked_trips_map = HashMap::new();
    for trip in sim.get_active_trips() {
        if trip.has_plan() {
            tracked_trips_map.insert(String::from(trip.get_id()), trip);
            tracked_trip_ids.push(String::from(trip.get_id()));
        }
    }

    let file = File::open(sim.get_od_path()).unwrap();
    let mut reader = csv::Reader::from_reader(file);

    for result in reader.deserialize() {
        // Get some needed values from the row
        let row: HashMap<String, String> = result.unwrap();
        let true_dep_id = row.get("true departure stop").unwrap();
        let true_arr_id = row.get("true arrival stop").unwrap();
        let id = String::from(row.get("ID").unwrap());

        if  true_dep_id == "" && true_arr_id == "" {
            // this trip should not have a plan
            assert!(! tracked_trip_ids.contains(&id));
        } else {
            // this trip should have a plan
            assert!(tracked_trip_ids.contains(&id));

            let trip = tracked_trips_map.get(&id).unwrap();

            // this trip must start and end at the specified stops
            let journey = trip.get_planned_ptlegs();

            println!("{:?} Trip: {}", env_dir, trip.get_id());
            assert_eq!(journey[0].board_stop_id, *true_dep_id);
            let journey_end = journey.last().unwrap();
            assert_eq!(journey_end.disembark_stop_id, *true_arr_id);
        }
    }
}

#[test]
/// tests that run works in some easy environments where every trip should be completed as planned.
fn test_run_simple() {
    let root_dir = PathBuf::from("tests/envs");
    for env_dir in &["pt-simple", "pt-multilines"] {
        println!("Testing {:?}", env_dir);
        let mut root_dir = root_dir.clone();
        root_dir.push(env_dir);

        let mut env_cfg_path = root_dir.clone();
        env_cfg_path.push("config.yaml");
        let sim = DynamicSimulator::new(env_cfg_path.to_str().unwrap()).unwrap();
        
        let (finished_trips, vehicles) = sim.run();
        check_vehicle_journeys_match_trips(&finished_trips, &vehicles, sim.get_transit_system());

        // count how many trips have valid paths.  This should be the number in finished_trips().
        let file = File::open(sim.get_od_path()).unwrap();
        let mut reader = csv::Reader::from_reader(file);
        let mut num_valid = 0;
        for result in reader.deserialize() {
            let row: HashMap<String, String> = result.unwrap();
            let true_dep_id = row.get("true departure stop").unwrap();
            let true_arr_id = row.get("true arrival stop").unwrap();
            if  true_dep_id != "" && true_arr_id != "" {
                num_valid += 1;
            }
        }
        assert_eq!(num_valid, finished_trips.len());

        // check that each trip's real journey is the same as its planned journey
        for trip in finished_trips {
            let planned_journey = trip.get_planned_journey();
            let real_journey = trip.get_real_journey();
            // there should be the same number of legs.
            assert_eq!(planned_journey.len(), real_journey.len());
            // first walking leg should be the same.
            assert_eq!(planned_journey[0], real_journey[0]);
            // last real leg should end at the same time or earlier than planned.
            let get_end_time = |jj: &Vec<JourneyLeg>| {
                match jj.last().unwrap() {
                    JourneyLeg::WalkingLeg(wl) => wl.end_time_s,
                    JourneyLeg::PtLeg(ptl) => ptl.disembark_time_s.unwrap(),    
                }
            };
            assert!(get_end_time(planned_journey) > get_end_time(real_journey));

            // real journey should follow the same route as planned journey
            for (planned, real) in trip.get_planned_journey().iter().zip(trip.get_real_journey()) {
                match planned {
                    JourneyLeg::PtLeg(planned_pt) => {
                        if let JourneyLeg::PtLeg(real_pt) = real {
                            assert_eq!(planned_pt.route_id, real_pt.route_id);
                            assert_eq!(planned_pt.board_stop_id, real_pt.board_stop_id);
                            assert_eq!(planned_pt.disembark_stop_id, real_pt.disembark_stop_id);
                        } else {
                            panic!("Planned leg was a pt leg, but real leg was walking!");
                        }
                    }
                    JourneyLeg::WalkingLeg(planned_walk) => {
                        if let JourneyLeg::WalkingLeg(real_walk) = real {
                            assert_eq!(planned_walk.start_pos, real_walk.start_pos);
                            assert_eq!(planned_walk.end_pos, real_walk.end_pos);
                            assert!(planned_walk.start_time_s >= real_walk.start_time_s);
                            assert!(planned_walk.end_time_s >= real_walk.end_time_s);
                        } else {
                            panic!("Planned leg was a walking leg, but real leg was pt!");
                        }
                    }
                }
            }
        }

        // compare vehicle journeys with schedules
        for vehicle in vehicles {
            let mut traversal_start_idx = 0;
            let real_journey = vehicle.get_real_journey();
            let schedule = vehicle.get_schedule();

            for (ii, (planned_start_time_s, route)) in schedule.iter().enumerate() {

                // route traversal should begin exactly when scheduled.
                let first_trav_leg = &real_journey[traversal_start_idx];
                assert_eq!(*planned_start_time_s, first_trav_leg.start_time_s);

                // route traversal should begin at the route's first stop.
                match &first_trav_leg.state {
                    VehicleState::AtStopOnRoute(_, stop_id) => 
                        assert_eq!(route.stops[0].facility_id, *stop_id),
                    _ => panic!("First vehicle leg on a traversal is not AtStopOnRoute!"),
                }
                
                let mut prev_stop_id = &route.stops[0].facility_id;
                let mut curr_leg_idx_offset = 1;
                for planned_stop in &route.stops[1..] {
                    // check that each stop in the planned route is accounted for in the real journey.
                    let curr_leg = &real_journey[traversal_start_idx + curr_leg_idx_offset];
                    if let VehicleState::Travelling(from_id, to_id) = &curr_leg.state {
                        assert_eq!(*from_id, *prev_stop_id);
                        assert_eq!(*to_id, planned_stop.facility_id);                        
                        prev_stop_id = &planned_stop.facility_id;
                    } else {
                        panic!("Leg {} should have been 'Travelling', but wasn't!", 
                               traversal_start_idx + curr_leg_idx_offset);
                    }
                    curr_leg_idx_offset += 1;
                    let curr_leg = &real_journey[traversal_start_idx + curr_leg_idx_offset];
                    if let VehicleState::AtStopOnRoute(_, stop_id) = &curr_leg.state {
                        // check that the stop id is correct.
                        assert_eq!(*stop_id, planned_stop.facility_id);
                        // advance past this stop-leg to the next travelling leg.
                        curr_leg_idx_offset += 1;
                    }
                }

                if ii < schedule.len() - 1 {
                    let next_route = schedule[ii + 1].1;
                    let next_route_first_stop = &next_route.stops[0].facility_id;
                    if *prev_stop_id != *next_route_first_stop {
                        // check for deadhead run between routes
                        let curr_leg = &real_journey[traversal_start_idx + curr_leg_idx_offset];
                        if let VehicleState::Travelling(from_id, to_id) = &curr_leg.state {
                            assert_eq!(*from_id, *prev_stop_id);
                            assert_eq!(*to_id, *next_route_first_stop);
                        } else {
                            panic!("Deadhead run is missing!");
                        }
                        curr_leg_idx_offset += 1;
                    }
                }

                // all vehicles in these scenarios should finish their routes ahead of time,
                // so assert that they end with Idle legs.
                let curr_leg = &real_journey[traversal_start_idx + curr_leg_idx_offset];
                match &curr_leg.state {
                    VehicleState::Idle(_) => {},
                    _ => panic!("Last leg should be Idle!"),
                }
                traversal_start_idx += curr_leg_idx_offset + 1;
            }
        }
    }
}

#[test]
fn test_run_multiple_boarders() {
    let root_dir = "tests/envs/pt-differentloads";
    for cfg_filename in &["config_multiboard.yaml", "config_boarddelays.yaml"] {
        let env_cfg_path: PathBuf = [root_dir, cfg_filename].iter().collect();
        let sim = DynamicSimulator::new(env_cfg_path.to_str().unwrap()).unwrap();
        let (trips, vehicles) = sim.run();

        // cross-correlate trips with vehicles
        check_vehicle_journeys_match_trips(&trips, &vehicles, sim.get_transit_system());

        // check that all of the trips were finished.
        let file = File::open(sim.get_od_path()).unwrap();
        let mut reader = csv::Reader::from_reader(file);    
        let mut true_num_finished = 0;

        // helper function
        let compare_deps = |journey: &Vec<JourneyLeg>, true_deps_str: &String| {
            let true_dep_ids: Vec<&str> = true_deps_str.split(",").collect();
            let mut dep_ids = vec![];
            for leg in journey {
                if let JourneyLeg::PtLeg(ptleg) = leg {
                    dep_ids.push(ptleg.departure_id.clone());
                }
            }
            assert_eq!(true_dep_ids, dep_ids);
        };

        for result in reader.deserialize() {
            let row: HashMap<String, String> = result.unwrap();
            let base_id = row.get("ID").unwrap();
            let succeeds = row.get("succeeds").unwrap().parse().unwrap();
            let expf = row.get("t_expf").unwrap().parse().unwrap();
            let trip_ids = PassengerTrip::ids_from_expansion_factor(&base_id, expf);
            for trip_id in trip_ids {
                let matched_trip = trips.iter().find(|tt| tt.id == trip_id).unwrap();

                // check that the trip was planned as expected
                compare_deps(matched_trip.get_planned_journey(), row.get("planned departure ids").unwrap());
                // check that the real trip went as expected
                compare_deps(matched_trip.get_real_journey(), row.get("real departure ids").unwrap());

                if succeeds {
                    true_num_finished += 1;

                    // check that the trip got off at its last planned stop
                    let real_pts = matched_trip.get_real_ptlegs();
                    let last_real_pt = real_pts.last().unwrap();
                    let planned_pts = matched_trip.get_planned_ptlegs();
                    let last_planned_pt = planned_pts.last().unwrap();
                    assert_eq!(last_planned_pt.disembark_stop_id, last_real_pt.disembark_stop_id);
                }
            }
        }

        let mut num_finished = 0;
        for trip in trips {
            if trip.is_finished() {
                num_finished += 1;
            }
        }
        assert_eq!(true_num_finished, num_finished);

        if *cfg_filename == "config_multiboard.yaml" {
            // check that all vehicles complete their journeys.
            for vehicle in vehicles {
                assert!(vehicle.is_finished());
            }        
        } else if *cfg_filename == "config_boarddelays.yaml" {
            for vehicle in vehicles {
                if vehicle.id == "1" {
                    assert!(! vehicle.is_finished());
                } else {
                    assert!(vehicle.is_finished());
                }
            }        
        }
    }
}

fn count_combinations(n: u64, r: u64) -> u64 {
    if r > n {
        0
    } else {
        (1..=r).fold(1, |acc, val| acc * (n - val + 1) / val)
    }
}

#[test]
fn test_samples_are_different() {
    let root_dir = "tests/envs/pt-differentloads";
    let cfg_filename = "config_subsample.yaml";
    let env_cfg_path: PathBuf = [root_dir, cfg_filename].iter().collect();
    let mut sim = DynamicSimulator::new(env_cfg_path.to_str().unwrap()).unwrap();

    let mut first_trip_ids = HashSet::new();
    for trip in sim.get_active_trips() {
        first_trip_ids.insert(trip.id.clone());
    }

    // calculate how many trials we'd need to have less than a 1 in 10^12 chance
    // of not seeing different sets of ids if it was truly random samples.
    let num_trips = sim.get_active_trips().len() as u64;
    let mut are_different = false;
    let tolerable_odds = (10 as f64).powi(12);
    let onetrial_odds = count_combinations(num_trips, num_trips / 2) as f64;
    let num_trials = tolerable_odds.log(onetrial_odds) as u32;

    // try this many times.
    for _ in 0..num_trials {
        sim.preprocess();
        let mut second_trip_ids = HashSet::new();
        for trip in sim.get_active_trips() {
            second_trip_ids.insert(trip.id.clone());
        }
        if first_trip_ids != second_trip_ids {
            // there was a difference, so we can stop.
            are_different = true;
            break;
        }
    }
    // odds of no difference 
    assert!(are_different);
}


fn check_vehicle_journeys_match_trips(trips: &Vec<PassengerTrip>, vehicles: &Vec<PtVehicle>, pt_system: &PtSystem) {
    // this will keep track of when the vehicles must have made stops, so we can
    // check it against them.
    let mut vehicles_stops = HashMap::new();
    
    // collect times when a passenger got on or off a stopped vehicle.
    for trip in trips {
        for real in trip.get_real_journey() {
            if let JourneyLeg::PtLeg(real_pt) = real {
                // track this board and disembark for the vehicles
                let real_veh_id = pt_system.get_vehicle_by_route_and_dep(&real_pt.route_id, &real_pt.departure_id)
                                           .unwrap();
                let real_veh_id = String::from(real_veh_id);
                let times = vehicles_stops.entry((real_veh_id.clone(), real_pt.board_stop_id.clone())).or_insert(vec![]);

                times.push(real_pt.board_time_s);
                if let Some(dt) = real_pt.disembark_time_s {
                    let times = vehicles_stops.entry((real_veh_id.clone(), real_pt.disembark_stop_id.clone()))
                                                     .or_insert(vec![]);
                    times.push(dt);
                }            
            }
        }
    }

    // sort stop times
    for (_, stop_times) in vehicles_stops.iter_mut() {
        stop_times.sort_unstable();
    }
    // compare vehicle journeys
    for vehicle in vehicles {
        let mut planned_stops = vec![];
        for (_, route) in vehicle.get_schedule() {
            for stop in &route.stops {
                planned_stops.push(stop);
            }
        }

        for leg in vehicle.get_real_journey() {
            if let VehicleState::AtStopOnRoute(_, stop_id) = &leg.state {
                // check that this corresponds to a time that a passenger got on or off.
                // remove any skipped planned stops that came before this one.
                while planned_stops[0].facility_id != *stop_id {
                    // the vehicle skipped this planned stop, so make sure they were allowed to.
                    assert!(! planned_stops[0].await_departure);
                    planned_stops.remove(0);
                };
                let planned_stop = planned_stops.remove(0);
                match vehicles_stops.get_mut(&(vehicle.id.clone(), stop_id.clone())) {
                    Some(times_at_stop) if times_at_stop.len() > 0 => {
                        let first_time = times_at_stop[0];

                        assert!(first_time >= leg.start_time_s, "Someone got on or off a vehicle before it left the stop!");
                        if let Some(end_time_s) = leg.end_time_s {
                            if ! planned_stop.await_departure {
                                // wasn't required to stop here, so vehicle must have stopped here 
                                // because someone was getting on or off.
                                assert!(first_time <= end_time_s, "Someone got on or off a vehicle after it left the stop!");
                            }
        
                            // remove any other times corresponding to this real stop.
                            while times_at_stop.len() > 0 && times_at_stop[0] <= end_time_s {
                                times_at_stop.remove(0);
                            }
                        }        
                    } 
                    _ => assert!(planned_stop.await_departure, "A vehicle stopped for no reason!")
                }
            }
        }
    }
}