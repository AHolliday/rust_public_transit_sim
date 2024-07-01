// standard library imports
use itertools::iproduct;
use rand::seq::SliceRandom;
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::path::{Path, PathBuf};

// non-standard crate imports
use kdtree::distance::squared_euclidean;
use kdtree::KdTree;
use rayon::prelude::*;
use yaml_rust::YamlLoader;
use ndarray::prelude::*;

extern crate blas_src;

// imports of other modules from this crate
use super::pt_system::StopFacility;
use super::pt_system::UniqueRouteId;
use super::pt_system::PtRoute;
use super::pt_system::{PtSystem, PtVehicle, VehicleState};
use super::passengers::{JourneyLeg, PassengerTrip, PtLeg, WalkingLeg};
use super::PathNetwork;
use super::SimConfig;
use super::geometry;
use super::config_utils;


pub struct Connection {
    pub dep_stop_id: String,
    pub dep_time_s: u32,
    pub arr_stop_id: String,
    pub arr_time_s: u32,
    pub route_id: UniqueRouteId,
    pub departure_id: String,
}

#[derive(Debug)]
struct ProfileEntry {
    dep_time_s: u32,
    dest_time_s: u32,
    // we use u32s for these indices since there will never be more than 2^32 connections,
    // stops, or profile entries
    connection_idx: u32,
    next_stop_idx: u32,
    next_profile_idx: Option<u32>,
    num_transfers: u8,
}

impl ProfileEntry {
    pub fn new(
        dep_time_s: u32,
        dest_time_s: u32,
        connection_idx: usize,
        next_stop_idx: usize,
        next_profile_idx: Option<usize>,
        num_transfers: u8,
    ) -> ProfileEntry {
        let next_profile_idx = match next_profile_idx {
            Some(idx) => Some(idx as u32),
            None => None,
        };

        ProfileEntry {
            dep_time_s,
            dest_time_s,
            connection_idx: connection_idx as u32,
            next_stop_idx: next_stop_idx as u32,
            next_profile_idx,
            num_transfers,
        }
    }

    pub fn get_connection_idx(&self) -> usize {
        self.connection_idx as usize
    }

    pub fn get_next_stop_idx(&self) -> usize {
        self.next_stop_idx as usize
    }

    pub fn get_next_profile_idx(&self) -> Option<usize> {
        match self.next_profile_idx {
            Some(idx) => Some(idx as usize),
            None => None,
        }
    }
}

struct QueuesAtStops {
    queues_by_stop: HashMap<String, HashMap<UniqueRouteId, VecDeque<PassengerTrip>>>,
}

impl QueuesAtStops {
    fn new(pt_system: &PtSystem) -> QueuesAtStops {
        let mut queues: HashMap<String, HashMap<UniqueRouteId, VecDeque<PassengerTrip>>> =
            HashMap::new();
        for stop_id in pt_system.stop_facilities.keys() {
            queues.insert(stop_id.clone(), HashMap::new());
        }

        QueuesAtStops {
            queues_by_stop: queues,
        }
    }

    fn get_queue(
        &mut self,
        stop_id: &str,
        route_id: &UniqueRouteId,
    ) -> &mut VecDeque<PassengerTrip> {
        let queues_for_stop = self.queues_by_stop.get_mut(stop_id).unwrap();
        queues_for_stop
            .entry(route_id.clone())
            .or_insert(VecDeque::new())
    }

    fn add_passenger(&mut self, trip: PassengerTrip) {
        if let Some(next_ptleg) = trip.get_next_planned_ptleg() {
            let queue = self.get_queue(&next_ptleg.board_stop_id, &next_ptleg.route_id);
            queue.push_back(trip);
        }
    }

    fn remove_passengers(
        &mut self,
        stop_id: &str,
        route_id: &UniqueRouteId,
        num: usize,
    ) -> Vec<PassengerTrip> {
        let queue = self.get_queue(stop_id, route_id);
        queue.drain(..num).collect()
    }

    fn remove_all_passengers(&mut self) -> Vec<PassengerTrip> {
        let mut passengers = vec![];
        for (_, stop_queues) in self.queues_by_stop.iter_mut() {
            for (_, queue) in stop_queues.iter_mut() {
                passengers.extend(queue.drain(..));
            }
        }
        passengers
    }

    fn get_queue_len(&mut self, stop_id: &str, route_id: &UniqueRouteId) -> usize {
        self.get_queue(stop_id, route_id).len()
    }

    fn remove_too_old_passengers(&mut self, time_s: u32, limit_s: u32) -> Vec<PassengerTrip> {
        let mut give_uppers = vec![];
        for (_, stop_queues) in &mut self.queues_by_stop {
            for (_, queue) in stop_queues {
                while let Some(trip) = queue.front() {
                    // make this an i32 to avoid overflow errors, since it might be *after* time_s
                    let time_at_stop = trip.get_time_arrived_at_stop().unwrap() as i32;
                    if time_s as i32 - time_at_stop >= limit_s as i32 {
                        // this trip has waited too long, so the passenger gives up!
                        give_uppers.push(queue.pop_front().unwrap());
                    } else {
                        // if a passenger in a queue has not waited too long, neither have any
                        // who arrived after them.
                        break;
                    }
                }
            }
        }
        give_uppers
    }
}

pub struct DynamicSimConfig {
    // path to an xml file describing the street network
    pub network_path: PathBuf,
    // path to an xml file describing the transit schedule
    pub pt_schedule_path: PathBuf,
    // path to an xml file describing the vehicles
    pub pt_vehicles_path: PathBuf,
    // path to the csv file describing the trips
    pub od_path: PathBuf,
    // radius around a trip's origin within which transit stops will be considered
    pub basin_radius_m: f64,
    // amount by which a passenger will put off starting their trip
    pub delay_tolerance_s: u32,
    // if passenger leaving for the first stop at their intended trip start time would have to
    // wait longer than this for their bus, they'll delay leaving.
    pub initial_wait_tolerance_s: u32,
    // time it takes to transfery between buses at the same bus stop
    pub transfer_time_s: u32,
    // max distance between stops that allow a transfer
    pub transfer_radius_m: f64,
    // approximate real walking distance between two points by multiplying crow-flies distance
    // by this number.
    pub beeline_dist_factor: f64,
    // walking speed used to estimate walking times.
    pub walk_speed_mps: f64,
    // maximum number of allowed transfers on a journey.
    pub max_transfers: Option<u8>,
    // average time we assume it takes for a vehicle to traverse an intersection
    pub mean_intersection_time_s: u32,
    // time it takes for a passenger to get on the bus.  Floating point since this will tend to be small.
    pub board_period_s: f64,
    // time it takes for a passenger to get off the bus.  Floating point since this will tend to be small.
    pub disembark_period_s: f64,
    // The random fraction of riders that will be considered by the simulator.
    // If 1, that's all of them. If 0.5, half of them.  etc.
    pub subsample_fraction: f64,
    // whether disconnected nodes in the street graph will simply be ignored
    ignore_small_components: bool,
}

impl SimConfig for DynamicSimConfig {
    fn get_basin_radius_m(&self) -> f64 {
        return self.basin_radius_m;
    }

    fn get_transfer_time_s(&self) -> u32 {
        return self.transfer_time_s;
    }

    fn get_transfer_radius_m(&self) -> f64 {
        return self.transfer_radius_m;
    }

    fn get_beeline_dist_factor(&self) -> f64 {
        return self.beeline_dist_factor;
    }

    fn get_walk_speed_mps(&self) -> f64 {
        return self.walk_speed_mps;
    }

    fn get_mean_intersection_time_s(&self) -> u32 {
        return self.mean_intersection_time_s;
    }

    fn get_ignore_small_components(&self) -> bool {
        return self.ignore_small_components;
    }
}


impl DynamicSimConfig {
    pub fn from_file(path: &str) -> DynamicSimConfig {
        let file_contents = std::fs::read_to_string(path).unwrap();
        let yaml_cfgs = YamlLoader::load_from_str(&file_contents).unwrap();
        let yaml_cfg = &yaml_cfgs[0];
        let config_dir = Path::new(path).parent().unwrap();

        let network_path = yaml_cfg["network_path"].as_str().unwrap();
        let network_path = config_utils::str_to_absolute_path(network_path, config_dir);

        let pt_schedule_path = yaml_cfg["pt_schedule_path"].as_str().unwrap();
        let pt_schedule_path = config_utils::str_to_absolute_path(pt_schedule_path, config_dir);

        let pt_vehicles_path = yaml_cfg["pt_vehicles_path"].as_str().unwrap();
        let pt_vehicles_path = config_utils::str_to_absolute_path(pt_vehicles_path, config_dir);

        let od_path = yaml_cfg["od_path"].as_str().unwrap();
        let od_path = config_utils::str_to_absolute_path(od_path, config_dir);

        DynamicSimConfig {
            network_path,
            pt_schedule_path,
            pt_vehicles_path,
            od_path,
            basin_radius_m: yaml_cfg["basin_radius_m"].as_f64().unwrap() as f64,
            delay_tolerance_s: yaml_cfg["delay_tolerance_s"].as_i64().unwrap() as u32,
            initial_wait_tolerance_s: yaml_cfg["initial_wait_tolerance_s"].as_i64().unwrap() as u32,
            transfer_time_s: yaml_cfg["transfer_time_s"].as_i64().unwrap() as u32,
            transfer_radius_m: yaml_cfg["transfer_radius_m"].as_f64().unwrap() as f64,
            beeline_dist_factor: yaml_cfg["beeline_dist_factor"].as_f64().unwrap() as f64,
            walk_speed_mps: yaml_cfg["walk_speed_mps"].as_f64().unwrap() as f64,
            max_transfers: if yaml_cfg["max_transfers"].is_null() {
                None
            } else {
                Some(yaml_cfg["max_transfers"].as_i64().unwrap() as u8)
            },
            mean_intersection_time_s: yaml_cfg["mean_intersection_time_s"].as_f64().unwrap() as u32,
            board_period_s: yaml_cfg["board_period_s"].as_f64().unwrap() as f64,
            disembark_period_s: yaml_cfg["disembark_period_s"].as_f64().unwrap() as f64,
            subsample_fraction: match yaml_cfg["subsample_fraction"].as_f64() {
                Some(sf) => sf as f64,
                None => 1.0,
            },
            ignore_small_components: yaml_cfg["ignore_small_components"].as_bool().
                expect("no ignore small components"),
        }
    }
}

pub struct DynamicSimulator {
    config: &'static DynamicSimConfig,
    network: PathNetwork<'static, DynamicSimConfig>,
    pt_system: PtSystem,
    connections: Vec<Connection>,
    profiles: HashMap<String, HashMap<String, Vec<ProfileEntry>>>,
    base_trips: Vec<PassengerTrip>,
    _active_trips: Vec<PassengerTrip>,
}

impl DynamicSimulator {
    pub fn new(config_path: &str) -> Result<DynamicSimulator, Box<dyn Error>> {
        let config = Box::new(DynamicSimConfig::from_file(config_path));
        let config: &'static mut DynamicSimConfig = Box::leak(config);

        // build the network from the network xml file.
        let network = match PathNetwork::from_xml(config.network_path.as_path(), config) {
            Ok(network) => network,
            Err(err) => panic!("{:?}", err),
        };

        // build the pt system from the vehicles and schedule xml files.
        let mut pt_system = PtSystem::from_xml_files(
            config.pt_vehicles_path.as_path(),
            config.pt_schedule_path.as_path(),
        );
        if config.subsample_fraction < 1.0 {
            pt_system.scale_capacities(config.subsample_fraction);
        }

        let connections = get_connections_from_pt_system(&pt_system);
        // TODO implement limiting time range of simulator based on config file
        let base_trips = PassengerTrip::all_from_csv(Path::new(&config.od_path), true, None, None)?;

        let mut sim = DynamicSimulator {
            config,
            network,
            pt_system,
            connections,
            profiles: HashMap::new(),
            base_trips,
            _active_trips: vec![],
        };

        sim.preprocess();

        Ok(sim)
    }

    pub fn get_od_path(&self) -> &Path {
        &self.config.od_path
    }

    pub fn set_transit_system(&mut self, new_pt_sys: PtSystem) {
        self.pt_system = new_pt_sys;
        self.connections = get_connections_from_pt_system(&self.pt_system);
        self.preprocess();
    }

    pub fn get_transit_system(&self) -> &PtSystem {
        &self.pt_system
    }

    /// Perform all preprocessing for running the simulation.
    pub fn preprocess(&mut self) {
        self.build_pcsa_profiles();
        self.sample_trips();
        self.assign_trips();
    }

    fn sample_trips(&mut self) {
        if self.config.subsample_fraction < 1.0 {
            self._active_trips.clear();
            let amount = (self.base_trips.len() as f64 * self.config.subsample_fraction) as usize;
            let mut rng = &mut rand::thread_rng();
            for elem in self.base_trips.choose_multiple(&mut rng, amount).cloned() {
                self._active_trips.push(elem)
            }
        }
        // else, we don't need to do anything, since get_active_trips should be
        // used.
    }

    pub fn get_active_trips(&self) -> &Vec<PassengerTrip> {
        if self.config.subsample_fraction == 1.0 {
            &self.base_trips
        } else {
            &self._active_trips
        }
    }

    pub fn get_active_trips_mut(&mut self) -> &mut Vec<PassengerTrip> {
        if self.config.subsample_fraction == 1.0 {
            &mut self.base_trips
        } else {
            &mut self._active_trips
        }
    }

    pub fn get_max_transfers(&self) -> Option<u8> {
        self.config.max_transfers
    }

    /// Implements the pre-processing step of the pareto-Connection Scan
    /// Algorithm of Dibbelt et al. (2013), modified to allow multiple start
    /// and end points, and to allow thresholding the allowed number of transfers.
    fn build_pcsa_profiles(&mut self) {
        log::info!("building pcsa profiles...");
        self.profiles.clear();

        // build a vector of stop ids and positions, to ensure a fixed order
        let stopfacs = &self.pt_system.stop_facilities;
        let mut stop_positions = vec![];
        let mut stop_ids = vec![];
        for (_, dest_sf) in self.pt_system.get_stopfacs_vec().iter().enumerate() {
            stop_positions.push(dest_sf.pos.clone());
            stop_ids.push(dest_sf.id.clone());
        }
        log::debug!("set up stop info");
        let stop_dists_mat = geometry::euclidean_distances(&stop_positions, None);

        // compute walk time and in-transfer-range matrices
        // we'll reference this often, so a shorthand will be useful.
        let in_range_mat = stop_dists_mat.mapv(|xx| xx <= self.config.transfer_radius_m);
        let walk_times_mat = stop_dists_mat.mapv(|xx| self.config.dist_to_walk_time(xx) as u32);
        log::debug!("computed walk times");

        // build the foottimes hash-map
        // TODO could this be a matrix instead?
        let mut foottimes = HashMap::new();
        for (ii, stop_id) in stop_ids.iter().enumerate() {
            // let in_range_row = in_range_mat.slice(s![ii, ..]);
            let mut stop_foottimes = vec![];
            let in_range_row = in_range_mat.slice(s![ii, ..]);

            // for (jj, is_in_range) in in_range_row.indexed_iter() {
            for (jj, is_in_range) in in_range_row.indexed_iter() {
                if *is_in_range {
                    let other_id = stop_ids[jj].clone();
                    let walk_time = walk_times_mat[[ii, jj]];
                    stop_foottimes.push((other_id, walk_time));
                }
            }
            foottimes.insert(stop_id.clone(), stop_foottimes);
        }
        log::debug!("built foottimes");

        // Aaaaaaand build the profiles in parallel!
        self.profiles = stopfacs
            .par_iter()
            .map(|(dest_stop_id, dest_sf)| {
                (
                    dest_stop_id.clone(),
                    self.build_profile_set_for_dest(dest_sf, &foottimes),
                )
            })
            .collect();
        log::info!("Finished building pCSA profiles");
    }

    fn get_profile_for_stops(
        &self,
        orig_sfid: &str,
        dest_sfid: &str,
    ) -> Option<&Vec<ProfileEntry>> {
        // Our profiles are stored "backwards": we index first by dest, then by orig.
        match self.profiles.get(dest_sfid) {
            Some(orig_profiles) => orig_profiles.get(orig_sfid),
            None => None,
        }
    }

    /// Performs the planning step of pCSA to find the quickest journey from the origin
    /// to the destination for each trip, starting from the given time.
    fn assign_trips(&mut self) {
        // Ceder 2011 (Transit network design methodology...) uses an
        // "exponential perception of distance" to allocate demand from
        // "transit centers" to nearby stops (eqn 5). Could this be a good model
        // to use to randomize ridership at a stop?  I should see if there is
        // some research on whether that is a realistic model of rider
        // behaviour.
        log::info!("Assigning {} trips!", self.get_active_trips().len());

        for trip in self.get_active_trips_mut() {
            trip.clear();
        }
        // filter the trips by proximity to stops
        let orig_stop_ids_by_trip = self.get_accessible_stops_by_trip("orig");
        let orig_trip_ids: HashSet<usize> = orig_stop_ids_by_trip.keys().cloned().collect();

        let dest_stop_ids_by_trip = self.get_accessible_stops_by_trip("dest");
        let dest_trip_ids: HashSet<usize> = dest_stop_ids_by_trip.keys().cloned().collect();

        // take the intersection of the sets
        let tracked_trip_ids: Vec<&usize> = orig_trip_ids.intersection(&dest_trip_ids).collect();
        log::debug!(
            "of {} trips, {} are in range of stops",
            self.get_active_trips().len(),
            tracked_trip_ids.len()
        );

        // plan the journey for each trip that's in range of a stop at each end
        let planned_journeys: Vec<Vec<JourneyLeg>> = tracked_trip_ids
            .par_iter()
            .map(|trip_id| {
                self.plan_trip(
                    &self.get_active_trips()[**trip_id],
                    &orig_stop_ids_by_trip[*trip_id],
                    &dest_stop_ids_by_trip[*trip_id],
                )
            })
            .collect();
        log::info!("Finished planning journeys");
        log::debug!("Planned journeys: {:?}", planned_journeys.len());

        // attach the planned journeys to their trips
        for (trip_id, planned_journey) in tracked_trip_ids.iter().zip(planned_journeys) {
            log::debug!("planned journey: {:?}", planned_journey);
            self.get_active_trips_mut()[**trip_id].set_planned_journey(planned_journey);
        }

        for trip in self.get_active_trips() {
            log::debug!("{:?}", trip.has_plan());
        }
    }

    fn get_accessible_stops_by_trip(&self, orig_or_dest: &str) -> HashMap<usize, Vec<String>> {
        let mut kdtree = KdTree::new(2);
        if orig_or_dest == "orig" {
            for (ii, trip) in self.get_active_trips().iter().enumerate() {
                kdtree.add(trip.origin.as_array(), ii).unwrap();
            }
        } else if orig_or_dest == "dest" {
            for (ii, trip) in self.get_active_trips().iter().enumerate() {
                kdtree.add(trip.destination.as_array(), ii).unwrap();
            }
        } else {
            panic!(
                r#"Arg orig_or_dest must be "orig" or "dest", but {} was given!"#,
                orig_or_dest
            );
        }

        let mut stops_by_trip = HashMap::new();
        for (sfid, stopfac) in &self.pt_system.stop_facilities {
            // find endpoints within basin radius of stops
            let within = kdtree
                .within(
                    &stopfac.pos.as_array(),
                    self.config.basin_radius_m.powi(2),
                    &squared_euclidean,
                )
                .unwrap();
            for (_, trip_idx) in within {
                let trip_stops = stops_by_trip.entry(*trip_idx).or_insert(vec![]);
                trip_stops.push(sfid.clone());
            }
        }

        stops_by_trip
    }

    fn plan_trip(
        &self,
        trip: &PassengerTrip,
        orig_stops: &Vec<String>,
        dest_stops: &Vec<String>,
    ) -> Vec<JourneyLeg> {
        let orig_walk_times: HashMap<String, u32> = orig_stops
            .iter()
            .map(|id| {
                let sf = self.pt_system.stop_facilities.get(id).unwrap();
                (id.clone(), self.config.get_walk_time(&sf.pos, &trip.origin) as u32)
            })
            .collect();

        let dest_walk_times: HashMap<String, u32> = dest_stops
            .iter()
            .map(|id| {
                let sf = self.pt_system.stop_facilities.get(id).unwrap();
                (
                    id.clone(),
                    self.config.get_walk_time(&sf.pos, &trip.destination) as u32,
                )
            })
            .collect();

        // find the best (orig, dest) stop pair
        let od_walk_time = self.config.get_walk_time(&trip.origin, &trip.destination) as u32;
        let mut best_time_s = trip.planned_start_time_s + od_walk_time;
        let mut best_od_stops = None;
        let mut first_entry_idx = None;

        for (orig_sfid, dest_sfid) in iproduct!(orig_stops, dest_stops) {
            if orig_sfid == dest_sfid {
                // Faster to just walk directly there.
                continue;
            }

            let mut od_first_entry_idx = None;
            let orig_stop = self.pt_system.stop_facilities.get(orig_sfid).unwrap();
            let dest_stop = self.pt_system.stop_facilities.get(dest_sfid).unwrap();

            let at_orig_stop_time =
                trip.planned_start_time_s + orig_walk_times.get(&orig_stop.id).unwrap();

            // get the profile for this orig,dest pair.
            let profile = match self.get_profile_for_stops(orig_sfid, dest_sfid) {
                Some(pp) => pp,
                None => continue,
            };

            // entries are in descending order of time, so iterate backwards, from earliest to
            // latest departures.
            for entry_idx in (0..profile.len()).rev() {
                let entry = &profile[entry_idx];
                let wait_time_s = entry.dep_time_s as i32 - trip.planned_start_time_s as i32;
                if wait_time_s > self.config.delay_tolerance_s as i32 {
                    // this journey starts too late!
                    break;
                // TODO allow starting sometime *before* the desired trip time?
                } else if entry.dep_time_s >= at_orig_stop_time {
                    // We found the first profile entry that stops after the person arrives at
                    // the orig stop.
                    od_first_entry_idx = Some(entry_idx);
                    break;
                }
            }

            let od_first_entry_idx = match od_first_entry_idx {
                Some(idx) => idx,
                None => continue,
            };

            let entry = &profile[od_first_entry_idx];
            // log::debug!("dest time: {}, walk time: {}", entry.dest_time_s, dest_walk_times.get(dest_sfid).unwrap());
            let arrival_time_s = entry.dest_time_s + dest_walk_times.get(dest_sfid).unwrap();
            // log::debug!("Considering {}, {} with time {}", orig_sfid, dest_sfid, arrival_time_s);
            if arrival_time_s < best_time_s {
                // log::debug!("Best yet!");
                best_time_s = arrival_time_s;
                best_od_stops = Some((orig_stop, dest_stop));
                first_entry_idx = Some(od_first_entry_idx);
            }
        }

        let (orig_stop, dest_stop) = match best_od_stops {
            Some(pair) => pair,
            None => return vec![],
        };

        // we've found the best orig, dest stop pair, now assemble the journey between them.

        let mut next_stop_id = &orig_stop.id;
        let mut route_conns: Vec<&Connection> = vec![];
        let mut next_entry_idx = first_entry_idx;

        // Start by adding the walk to the first stop.
        let walk_dur_s = orig_walk_times.get(&orig_stop.id).unwrap();
        let profile = self
            .get_profile_for_stops(&orig_stop.id, &dest_stop.id)
            .unwrap();
        let entry = &profile[next_entry_idx.unwrap()];
        let board_time_s = self.connections[entry.get_connection_idx()].dep_time_s;
        let time_at_first_stop_s = match trip.planned_start_time_s + walk_dur_s {
            sum if board_time_s - sum < self.config.initial_wait_tolerance_s => sum,
            _ => board_time_s - self.config.initial_wait_tolerance_s,
        };
        let first_walk = WalkingLeg {
            start_pos: trip.origin.clone(),
            end_pos: orig_stop.pos.clone(),
            start_time_s: time_at_first_stop_s - walk_dur_s,
            end_time_s: time_at_first_stop_s,
        };
        let mut plan = vec![JourneyLeg::WalkingLeg(first_walk)];

        // now iterate over the connections used on this trip and build the rest of the plan
        while *next_stop_id != dest_stop.id {
            let profile = self
                .get_profile_for_stops(&next_stop_id, &dest_stop.id)
                .unwrap();
            let entry = &profile[next_entry_idx.unwrap()];
            let cc = &self.connections[entry.get_connection_idx()];
            next_stop_id = &self
                .pt_system
                .get_stopfac_by_index(entry.get_next_stop_idx())
                .unwrap()
                .id;
            next_entry_idx = entry.get_next_profile_idx();

            if route_conns.len() > 0 && route_conns.last().unwrap().route_id != cc.route_id {
                // there has been a transfer, or we've reached the end!
                // add the transit journey leg...
                let ptleg = PtLeg::from_connections(route_conns[0], route_conns.last().unwrap());
                let start_pos = self
                    .pt_system
                    .stop_facilities
                    .get(&ptleg.disembark_stop_id)
                    .unwrap()
                    .pos
                    .clone();
                let start_time_s = ptleg.disembark_time_s.unwrap();
                let disembark_stop_id = ptleg.disembark_stop_id.clone();
                plan.push(JourneyLeg::PtLeg(ptleg));
                route_conns.clear();

                if disembark_stop_id != cc.dep_stop_id {
                    // ...and a walking journey leg if we had to walk between stops
                    let end_pos = self
                        .pt_system
                        .stop_facilities
                        .get(&cc.dep_stop_id)
                        .unwrap()
                        .pos
                        .clone();
                    let end_time_s = start_time_s + 
                        self.config.get_walk_time(&start_pos, &end_pos) as u32;
                    let walk = WalkingLeg {
                        start_pos,
                        end_pos,
                        start_time_s,
                        end_time_s,
                    };
                    plan.push(JourneyLeg::WalkingLeg(walk));
                }
            }

            route_conns.push(&cc);
        }

        if route_conns.len() > 0 {
            // Add the final pt journey
            let ptleg = PtLeg::from_connections(route_conns[0], route_conns.last().unwrap());

            let start_pos = self
                .pt_system
                .stop_facilities
                .get(&ptleg.disembark_stop_id)
                .unwrap()
                .pos
                .clone();
            let start_time_s = ptleg.disembark_time_s.unwrap();
            plan.push(JourneyLeg::PtLeg(ptleg));

            // add the final walking leg
            let end_pos = trip.destination.clone();
            let end_time_s = start_time_s + self.config.get_walk_time(&start_pos, &end_pos) as u32;
            let walk = WalkingLeg {
                start_pos,
                end_pos,
                start_time_s,
                end_time_s,
            };
            plan.push(JourneyLeg::WalkingLeg(walk));
        }

        plan
    }

    fn build_profile_set_for_dest(
        &self,
        dest_sf: &StopFacility,
        foottimes: &HashMap<String, Vec<(String, u32)>>,
    ) -> HashMap<String, Vec<ProfileEntry>> {
        let mut dest_profiles: HashMap<String, Vec<ProfileEntry>> = HashMap::new();

        // build the profiles!
        for (ci, cc) in self.connections.iter().enumerate().rev() {
            if cc.dep_stop_id == *dest_sf.id {
                continue;
            }

            let mut new_entry: Option<ProfileEntry> = None;

            // iterate over possible connecting stop facilities
            let stop_foottimes = foottimes.get(&cc.arr_stop_id).unwrap();
            for (csid, walk_time) in stop_foottimes {
                let mut valid_journey_found = false;
                let mut dest_time_s = 0;
                let mut next_profile_idx = None;
                let mut num_transfers = 0;

                if *csid == *dest_sf.id {
                    // we can walk to the destination stop from the conn's endpoint
                    valid_journey_found = true;
                    dest_time_s = cc.arr_time_s + walk_time;
                } else {
                    // This is not the destination stop
                    let arr_profile = match dest_profiles.get(csid) {
                        Some(profile) => profile,
                        None => continue,
                    };

                    // find the best journey starting from the candidate stop that gets to dest
                    for (ei, arr_entry) in arr_profile.iter().enumerate().rev() {
                        let mut transfer_time_s = cc.arr_time_s;
                        let entry_cc = &self.connections[arr_entry.get_connection_idx()];
                        if entry_cc.route_id != cc.route_id {
                            // this is a transfer!
                            num_transfers = arr_entry.num_transfers + 1;

                            if let Some(mt) = self.config.max_transfers {
                                if num_transfers > mt {
                                    continue;
                                }
                            } else if *csid == cc.arr_stop_id {
                                // this is a transfer at the same stop
                                transfer_time_s += self.config.transfer_time_s;
                            } else {
                                // this is a transfer between stops
                                transfer_time_s += walk_time;
                            }
                        }

                        if arr_entry.dep_time_s >= transfer_time_s {
                            valid_journey_found = true;
                            dest_time_s = arr_entry.dest_time_s;
                            next_profile_idx = Some(ei);
                            break;
                        }
                    }
                }

                if valid_journey_found {
                    if match new_entry.as_ref() {
                        Some(ne) => dest_time_s < ne.dest_time_s,
                        None => true,
                    } {
                        new_entry = Some(ProfileEntry::new(
                            cc.dep_time_s,
                            dest_time_s,
                            ci,
                            self.pt_system.get_index_of_stopfac(csid).unwrap(),
                            next_profile_idx,
                            num_transfers,
                        ));
                    }
                }
            }

            if let Some(new_entry) = new_entry {
                let profile = dest_profiles
                    .entry(cc.dep_stop_id.clone())
                    .or_insert(vec![]);

                if profile.len() == 0
                    || profile.last().unwrap().dest_time_s > new_entry.dest_time_s
                    || profile.last().unwrap().dep_time_s < new_entry.dep_time_s
                {
                    profile.push(new_entry);
                }
            }
        }

        dest_profiles
    }

    pub fn run(&self) -> (Vec<PassengerTrip>, Vec<PtVehicle>) {
        // initialize the state of the running simulator
        let mut queues_by_stop = QueuesAtStops::new(&self.pt_system);

        // initialize per-vehicle schedules
        let mut vehicle_schedules: HashMap<String, Vec<(u32, &PtRoute)>> =
            HashMap::new();
        for (_, route) in &self.pt_system.routes {
            for departure in &route.departures {
                let vehicle_id = departure.vehicle_id.clone();
                let schedule = vehicle_schedules.entry(vehicle_id).or_insert(vec![]);
                schedule.push((departure.start_time_s, route));
            }
        }

        // initialize data structures for vehicles
        let mut vehicles_by_activate_time: HashMap<u32, Vec<PtVehicle>> = HashMap::new();
        for (vehicle_id, veh_type_id) in &self.pt_system.vehicles {
            let veh_type = self.pt_system.vehicle_types.get(veh_type_id).unwrap();
            if let Some(mut schedule) = vehicle_schedules.remove(vehicle_id) {
                // sort the schedule by the starting time of each route.  Unstable is faster, and
                // there shouldn't be routes starting at the same time anyway.
                schedule.sort_unstable_by(|aa, bb| aa.0.partial_cmp(&bb.0).unwrap());
                // this vehicle is used in the present transit system, so set up its state
                let vehicle = PtVehicle::new(vehicle_id, veh_type, schedule);
                let first_start_time_s = vehicle.get_schedule()[0].0;
                let vehicles_starting_now = vehicles_by_activate_time
                    .entry(first_start_time_s)
                    .or_insert(vec![]);
                vehicles_starting_now.push(vehicle);
            }
        }

        let mut trips_by_atstop_time: HashMap<u32, Vec<PassengerTrip>> = HashMap::new();
        for trip in self.get_active_trips() {
            if trip.has_plan() {
                // Ignore trips that don't have public-transport plans.
                let mut state_trip = trip.clone();
                let act_time_s = state_trip.start_journey();
                let timevec = trips_by_atstop_time.entry(act_time_s).or_insert(vec![]);
                timevec.push(state_trip);
            }
        }
        let mut finished_trips = vec![];
        let mut finished_vehicles = vec![];

        // Timesteps are one second.
        let num_timesteps = 3600 * 24;
        let mut tt = 0;

        while vehicles_by_activate_time.len() > 0 {
            tt += 1;

            // add a new waiting passenger for each new trip that
            if let Some(trips) = trips_by_atstop_time.remove(&tt) {
                for trip in trips {
                    let next_ptleg = &trip.get_next_planned_ptleg().unwrap();
                    let queue =
                        queues_by_stop.get_queue(&next_ptleg.board_stop_id, &next_ptleg.route_id);
                    queue.push_back(trip);
                }
            }

            // for each vehicle that departs at this time:
            let vehicles = match vehicles_by_activate_time.remove(&tt) {
                Some(vehicles) => vehicles,
                None => vec![],
            };
            for mut veh in vehicles {
                let (stop_id, route_id) = veh.get_current_or_next_stop();
                let queue_size = queues_by_stop.get_queue_len(stop_id, route_id);
                veh.update_state(tt, queue_size > 0, self.config.disembark_period_s);

                let next_veh_activate_time = match veh.get_state() {
                    VehicleState::AtStopOnRoute(_, stop_id) => {
                        // NOTE: this makes sense as long as boarding period >= disembarking 
                        // period.  Otherwise, we could potentially have the bus exceed its
                        // maximum capacity for a time while at the stop.  That's a pretty trivial
                        // approximation though, and it should be that boarding period >=
                        // disembarking period in general anyway.

                        // handle disembarkers
                        let mut disembark_delay = 0.0;
                        let stop_id = stop_id.clone();
                        for mut trip in veh.disgorge_disembarkers(&stop_id) {
                            disembark_delay += self.config.disembark_period_s;
                            let disembark_time = tt + disembark_delay as u32;
                            trip.disembark(disembark_time);

                            if let Some(next_ptleg) = trip.get_next_planned_ptleg() {
                                if next_ptleg.board_stop_id == *stop_id {
                                    // put it in the stop queue, because we're already at the stop
                                    queues_by_stop.add_passenger(trip);
                                } else {
                                    // put trip in activation dict at the time when it'll reach the
                                    // next stop
                                    let at_next_stop_time_s =
                                        disembark_time + trip.get_next_stop_walk_duration();
                                    let vec = trips_by_atstop_time
                                        .entry(at_next_stop_time_s)
                                        .or_insert(vec![]);
                                    vec.push(trip);
                                }
                            } else {
                                // the trip is finished, it just has a walk left.
                                finished_trips.push(trip);
                            }
                        }

                        // handle boarders
                        let mut board_time = tt as f64;
                        let num_boarders = min(queue_size, veh.get_room() as usize);
                        let (_, route_id) = veh.get_current_or_next_stop();
                        let boarders =
                            queues_by_stop.remove_passengers(&stop_id, route_id, num_boarders);
                        for mut trip in boarders {
                            board_time += self.config.board_period_s;
                            if let Some(dep_id) = veh.get_current_departure_id() {
                                trip.board(board_time.ceil() as u32, dep_id);
                                veh.add_rider(trip);
                            } else {
                                panic!(
                                    "trip {:?} tried to board a vehicle that was not on a route!"
                                );
                            }
                        }

                        // activate after we're done boarding or one second later, whichever is
                        // sooner.
                        Some(max(tt + 1, board_time.ceil() as u32))
                    }
                    VehicleState::Travelling(from_id, to_id) => {
                        // reactivate after driving to the next stop
                        let from_sf = &self.pt_system.stop_facilities.get(&from_id).unwrap();
                        let to_sf = &self.pt_system.stop_facilities.get(&to_id).unwrap();
                        let travel_time = self
                            .network
                            .estimate_time_between_stops(from_sf, to_sf, veh.get_mode())
                            .unwrap();
                        // if the travel time is zero, make it 1 so we get activated next second.
                        let next_time = tt + max(travel_time, 1);
                        if veh.is_deadhead() && next_time > num_timesteps {
                            // If the next route is supposed to start after midnight, cancel!
                            None
                        } else {
                            Some(next_time)
                        }
                    }
                    VehicleState::Idle(start_next_route_time_s) => {
                        // reactivate when it's time to drive the next route.
                        if let Some(time_s) = start_next_route_time_s {
                            if time_s > num_timesteps {
                                // If the next route is supposed to start after midnight, cancel!
                                None
                            } else {
                                start_next_route_time_s
                            }
                        } else {
                            None
                        }
                    }
                };

                if let Some(nat) = next_veh_activate_time {
                    let veh_list = vehicles_by_activate_time.entry(nat).or_insert(vec![]);
                    veh_list.push(veh);
                } else {
                    finished_trips.append(&mut veh.empty_all_passengers());
                    finished_vehicles.push(veh);
                }
            }

            let mut give_uppers =
                queues_by_stop.remove_too_old_passengers(tt, self.config.delay_tolerance_s);
            finished_trips.append(&mut give_uppers);
        }

        // we've completed the day.  Return all trips and vehicles.

        // add all vehicles waiting to activate to finished vehicles, and remove all trips from
        // vehicles and add them to finished_trips.
        for (_, vehicles) in vehicles_by_activate_time {
            for mut veh in vehicles {
                finished_trips.append(&mut veh.empty_all_passengers());
                finished_vehicles.push(veh);
            }
        }
        // remove all trips from stop queues and add them to finished_trips
        finished_trips.append(&mut queues_by_stop.remove_all_passengers());

        // remove all trips that are waiting to activate and add them to finished trips
        for (_, mut trips) in trips_by_atstop_time {
            finished_trips.append(&mut trips);
        }
        log::info!("Simulator run finished after {} timesteps", tt);

        (finished_trips, finished_vehicles)
    }
}

fn get_connections_from_pt_system(ptsys: &PtSystem) -> Vec<Connection> {
    // build the connections list
    let mut connections = vec![];

    for (route_id, route) in &ptsys.routes {
        for dep in &route.departures {
            for si in 0..(route.stops.len() - 1) {
                let dep_stop = &route.stops[si];
                let arr_stop = &route.stops[si + 1];
                let connection = Connection {
                    dep_stop_id: dep_stop.facility_id.clone(),
                    // Every stop but the last one should have a departure offset,
                    // and we don't treat the last one as a dep_stop
                    dep_time_s: dep.start_time_s + dep_stop.get_departure_offset_s(),
                    arr_stop_id: arr_stop.facility_id.clone(),
                    arr_time_s: dep.start_time_s + arr_stop.get_arrival_offset_s(),
                    route_id: route_id.clone(),
                    departure_id: dep.id.clone(),
                };
                connections.push(connection);
            }
        }
    }
    // sort the connections list by departure time
    connections.sort_by(|aa, bb| aa.dep_time_s.cmp(&bb.dep_time_s));

    connections
}