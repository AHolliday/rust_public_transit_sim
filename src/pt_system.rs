use std::io::prelude::*;
use std::fs::File;
use std::path::Path;
use std::fmt::Debug;
use std::collections::HashMap;

use xml::reader::EventReader;
use xml::reader::XmlEvent;

use super::geometry::Point2d;
use super::PassengerTrip;
use super::config_utils;


pub struct PtSystem {
    pub vehicles: HashMap<String, String>,
    pub vehicle_types: HashMap<String, PtVehicleType>,
    pub routes: HashMap<UniqueRouteId, PtRoute>,
    pub stop_facilities: HashMap<String, StopFacility>,
    stopfac_ids_by_idx: Vec<String>,
    stopfac_idxs_by_id: HashMap<String, usize>,
}

impl PtSystem {
    pub fn from_xml_files(vehicles_xml_path: &Path, schedule_xml_path: &Path) -> Self {
        let mut ff = File::open(vehicles_xml_path).unwrap();
        let mut vehicles_xml = String::new();
        ff.read_to_string(&mut vehicles_xml).unwrap();

        let mut ff = File::open(schedule_xml_path).unwrap();
        let mut schedule_xml = String::new();
        ff.read_to_string(&mut schedule_xml).unwrap();

        Self::from_xml(&vehicles_xml, &schedule_xml)
    }

    pub fn from_xml(vehicles_xml: &str, schedule_xml: &str) -> PtSystem {
        // parse the xmls
        let (vehicles, vehicle_types) = parse_vehicles_xml(&vehicles_xml);
        let (routes, stop_facilities) = parse_schedule_xml(&schedule_xml);

        // these two collections allow efficient exchange between IDs and numerical indexes for
        // stop facilities, important because it allows key optimizations.
        let stopfac_ids_by_idx: Vec<String> = stop_facilities.keys().map(|ss| ss.clone())
                                                                    .collect();
        let stopfac_idxs_by_id: HashMap<String, usize> =
            stopfac_ids_by_idx.iter().enumerate().map(|(ii, id)| (id.clone(), ii)).collect();

        PtSystem {
            vehicles,
            vehicle_types,
            routes,
            stop_facilities,
            stopfac_ids_by_idx,
            stopfac_idxs_by_id
        }
    }

    pub fn get_stopfac_by_index(&self, index: usize) -> Option<&StopFacility> {
        if index >= self.stopfac_ids_by_idx.len() {
            return None;
        }
        let sfid = &self.stopfac_ids_by_idx[index];
        self.stop_facilities.get(sfid)
    }

    pub fn get_index_of_stopfac(&self, query_id: &str) -> Option<usize>{
        match self.stopfac_idxs_by_id.get(query_id) {
            Some(size) => Some(*size),
            None=> None,
        }
    }

    pub fn get_stopfacs_vec(&self) -> Vec<&StopFacility> {
        self.stopfac_ids_by_idx.iter().enumerate()
                                      .map(|(ii, _)| self.get_stopfac_by_index(ii).unwrap())
                                      .collect()
    }

    pub fn get_vehicle_by_route_and_dep(&self, route_id: &UniqueRouteId, dep_id: &str) -> Option<&str> {
        match self.routes.get(route_id) {
            Some(route) => {
                match route.departures.iter().find(|dd| dd.id == dep_id) {
                    Some(dep) => Some(&dep.vehicle_id),
                    None => None,
                }
            }
            None => None,
        }
    }

    pub fn scale_capacities(&mut self, scale: f64) {
        // iterate over vehicle types, and change their capacities.
        for veh_type in self.vehicle_types.values_mut() {
            veh_type.scale_capacity(scale);
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct PtVehicleType {
    pub id: String,
    pub mode: String,
    pub description: String,
    pub num_seats: usize,
    scaled_num_seats: f64,
    pub standing_room: usize,
    scaled_standing_room: f64,
    pub length_m: f64,
    // average power consumed by the vehicle while on a trip. Default is
    // estimate for a standard city bus
    pub avg_power_kW: f64,
}

impl PtVehicleType {
    pub fn new(id: &str, mode: &str, desc: &str, num_seats: usize, standing_room: usize,
               length_m: f64, avg_power_kW: f64) -> PtVehicleType {
        PtVehicleType {
            id: String::from(id),
            mode: String::from(mode),
            description: String::from(desc),
            num_seats,
            scaled_num_seats: num_seats as f64,
            standing_room,
            scaled_standing_room: standing_room as f64,
            length_m,
            avg_power_kW,
        }
    }

    fn from_xml_events(events: Vec<XmlEvent>) -> PtVehicleType {
        // define some closures that will be used many times in this function
        let right_elem_closure =
            |elem: &XmlEvent, query_name| match *elem {
                XmlEvent::StartElement{ ref name, .. }
                if name.local_name == query_name => true,
                _ => false,
            };

        let get_value = |element_name, attribute_name| {
            let found = events.iter().find(
                |xx| right_elem_closure(xx, element_name))
                .unwrap();

            if let XmlEvent::StartElement {name: _, attributes, ..} = found {
                let err_msg = format!("Xml vehicle type has no {:?} attr!",
                    attribute_name);
                config_utils::get_xml_attribute_value(attributes, attribute_name)
                    .expect(&err_msg)
            } else {
                panic!("Wrong element type found!");
            }
        };

        // get id
        let id = get_value("vehicleType", "id");

        // get length
        let length_m = get_value("length", "meter").parse().unwrap();

        // get num seats
        let num_seats = get_value("seats", "persons").parse().unwrap();

        // get standing room
        let standing_room = get_value("standingRoom", "persons").parse().unwrap();

        // get description and power
        let mut description = String::new();
        let mut avg_power_kW = 0.0;
        let mut power_specified = false;
        let mut mode = "";
        let mut mode_specified = false;
        if let Some(desc_start_idx) = events.iter().position(
            |xx| right_elem_closure(xx, "description")) {
            if let XmlEvent::Characters(descstr) = &events[desc_start_idx + 1] {

                let parts: Vec<&str> = descstr.split("--").collect();
                description = String::from(parts[0]);
                if parts.len() > 1 {
                    for part in parts {
                        let key_and_value: Vec<&str> = part.split("=").collect();
                        if key_and_value.len() == 2 {
                            let key = key_and_value[0];
                            let value = key_and_value[1];
                            // avg_power_kW = power_parts[1].parse().unwrap();
                            if key == "avg_power_kW" {
                                avg_power_kW = value.parse().unwrap();
                                power_specified = true;
                            } else if key == "mode" {
                                mode = value;
                                mode_specified = true;
                            }
                        }
                    }
                }

                // if parts.len() > 1 {
                //     let power_parts: Vec<&str> = parts[1].split("=").collect();
                //     avg_power_kW = power_parts[1].parse().unwrap();
                //     power_specified = true;
                // }
            }
        }

        if !power_specified {
            log::warn!("No power consumption was specified for vehicle {}!", id);
        }
        if !mode_specified {
            log::warn!("No transport mode was specified for vehicle {}!", id);
        }

        // TODO don't use a fixed mode "road", instead get it from config somehow
        PtVehicleType::new(
            &id,
            mode,
            &description,
            num_seats,
            standing_room,
            length_m,
            avg_power_kW)
    }

    pub fn scale_capacity(&mut self, scale: f64) {
        // this is used when we're subsampling trips in the pt system.
        // don't try to scale up
        assert!(scale <= 1.0);
        self.scaled_num_seats = self.num_seats as f64 * scale;
        self.scaled_standing_room = self.standing_room as f64 * scale;
    } 

    pub fn get_capacity(&self) -> usize {
        (self.scaled_num_seats + self.scaled_standing_room).round() as usize
    }
}

#[derive(PartialEq, Debug)]
pub struct StopFacility {
    pub id: String,
    pub link: String,
    pub pos: Point2d,
}

#[derive(PartialEq, Debug)]
pub struct PtPlannedStop {
    pub facility_id: String,
    arrival_offset_s: Option<u32>,
    departure_offset_s: Option<u32>,
    pub await_departure: bool,
}

impl PtPlannedStop {
    fn new(facility_id: String, arr_offset: Option<u32>, dep_offset: Option<u32>, 
           await_departure: bool) -> PtPlannedStop {
        if let None = arr_offset {
            if let None = dep_offset {
                panic!("One of arrival offset and departure offset must not be None!");
            }
        }

        if ! await_departure {
            log::warn!("await_departure = false is not supported by the route planner in this
                        version of the simulator!  Passengers will miss vehicles!");
        }

        PtPlannedStop {
            facility_id,
            arrival_offset_s: arr_offset,
            departure_offset_s: dep_offset,
            await_departure,
        }
    }

    pub fn get_arrival_offset_s(&self) -> u32 {
        match self.arrival_offset_s {
            // departure offset must be valid if arrival offset is not, as enforced
            // by the constructor
            Some(aos) => aos,
            None => 0,
        }
    }

    pub fn get_departure_offset_s(&self) -> u32 {
        match self.departure_offset_s {
            // departure offset must be valid if arrival offset is not, as enforced
            // by the constructor
            Some(dos) => dos,
            None => self.arrival_offset_s.unwrap(),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Departure {
    pub id: String,
    pub start_time_s: u32,
    pub vehicle_id: String,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct UniqueRouteId {
    pub line_id: String,
    pub route_id: String,
}

#[derive(PartialEq, Debug)]
pub struct PtRoute {
    pub id: UniqueRouteId,
    pub mode: String,
    pub stops: Vec<PtPlannedStop>,
    pub links: Vec<String>,
    pub departures: Vec<Departure>,
}


#[derive(PartialEq, Debug, Clone)]
pub enum VehicleState {
    Idle(Option<u32>),
    AtStopOnRoute(u32, String),
    Travelling(String, String),
}

#[derive(PartialEq, Debug)]
pub struct VehicleLeg {
    pub state: VehicleState,
    pub start_time_s: u32,
    pub end_time_s: Option<u32>,
    pub route_id: Option<UniqueRouteId>,
    pub departure_id: Option<String>,
}

#[derive(PartialEq, Debug)]
/// Unlike other structs in this module, this struct is used to keep track of
/// vehicles moving around in the simulator.
pub struct PtVehicle<'a> {
    pub id: String,
    vehicle_type: &'a PtVehicleType,
    schedule: Vec<(u32, &'a PtRoute)>,
    state: VehicleState,
    riders: HashMap<String, Vec<PassengerTrip>>,
    route_idx: usize,
    stop_idx: usize,
    real_journey: Vec<VehicleLeg>,
}

impl<'a> PtVehicle<'a> {
    pub fn new(id: &str, vehicle_type: &'a PtVehicleType,
           schedule: Vec<(u32, &'a PtRoute)>) -> PtVehicle<'a> {
        let first_activate_time = schedule[0].0;
        let state = VehicleState::Idle(Some(first_activate_time));
        PtVehicle {
            id: String::from(id), 
            vehicle_type,
            schedule,
            state,
            riders: HashMap::new(),
            route_idx: 0,
            stop_idx: 0,
            real_journey: vec![],
        }
    }

    pub fn get_state(&self) -> VehicleState {
        self.state.clone()
    }

    pub fn get_mode(&self) -> &str {
        &self.vehicle_type.mode
    }

    pub fn get_schedule(&self) -> &Vec<(u32, &'a PtRoute)> {
        &self.schedule
    }

    pub fn get_real_journey(&self) -> &Vec<VehicleLeg> {
        &self.real_journey
    }

    pub fn is_deadhead(&self) -> bool {
        if let VehicleState::Travelling(_, _) = self.state {
            return self.stop_idx == 0;
        }
        false
    }

    pub fn add_rider(&mut self, trip: PassengerTrip) {
        // rider should not be added if the bus is full!
        assert!(self.get_room() > 0, "Tried to fill bus over capacity!");
        let disembark_stop_id = trip.get_next_planned_ptleg().unwrap()
                                    .disembark_stop_id.clone();
        let stop_riders = self.riders.entry(disembark_stop_id).or_insert(vec![]);
        stop_riders.push(trip);
    }

    pub fn get_current_departure_id(&self) -> Option<&str> {
        if self.is_deadhead() || self.is_finished() {
            return None;
        }

        let (planned_start_time, route) = &self.schedule[self.route_idx];
        let dep = route.departures.iter().find(|dd| dd.start_time_s == *planned_start_time)
                                  .unwrap();
        Some(&dep.id)
    }

    pub fn get_current_route_id(&self) -> Option<&UniqueRouteId> {
        if self.is_deadhead() || self.is_finished() {
            return None;
        }

        let (_, route) = &self.schedule[self.route_idx];
        Some(&route.id)
    }

    /// Gives the current stop and route if we're at a stop; if not, 
    /// gives the next stop at which we'll arrive, and the route we'll be on.
    pub fn get_current_or_next_stop(&self) -> (&String, &UniqueRouteId) {
        let route = &self.schedule[self.route_idx].1;
        let stop_id = &route.stops[self.stop_idx].facility_id;
        (stop_id, &route.id)
    }

    pub fn disgorge_disembarkers(&mut self, stop_id: &str) -> Vec<PassengerTrip> {
        match self.riders.remove(stop_id) {
            Some(vec) => vec,
            None => vec![],
        }
    }

    pub fn empty_all_passengers(&mut self)  -> Vec<PassengerTrip> {
        let mut passengers = vec![];
        for (_, riders) in self.riders.iter_mut() {
            passengers.append(riders);
        }
        passengers
    }

    pub fn get_room(&self) -> usize {
        let num_riders = self.riders.iter().map(|(_, vv)| vv.len())
                                           .sum::<usize>();
        self.vehicle_type.get_capacity() - num_riders
    }

    pub fn is_finished(&self) -> bool {
        self.route_idx == self.schedule.len()
    }

    /// Move forward the vehicle's internal tracking of which part of its schedule it's engaged in.
    fn advance_schedule_to_next_stop(&mut self) {
        if self.is_finished() {
            return;
        }
        let planned_stops = &self.schedule[self.route_idx].1.stops; 
        let prev_stop_id = &planned_stops[self.stop_idx].facility_id;

        // if we're not done yet...
        self.stop_idx += 1;
        if self.stop_idx == planned_stops.len() {
            // we're done the current route, so switch to the next route.
            self.stop_idx = 0;
            self.route_idx += 1;
        }

        if self.is_finished() {
            self.state = VehicleState::Idle(None);
        } else {
            let (next_stop_id, _) = self.get_current_or_next_stop();
            self.state = VehicleState::Travelling(prev_stop_id.clone(), next_stop_id.clone());
        }
    }

    pub fn update_state(&mut self, time_s: u32, current_stop_has_queue: bool,
                    disembark_period_s: f64) {
        // what are the things that could be happening on activation?
        let old_state = self.state.clone();
        match &old_state {
            VehicleState::AtStopOnRoute(allowed_dep_time_s, _) => {
                if self.get_room() == 0 || 
                   (! current_stop_has_queue && time_s >= *allowed_dep_time_s) {
                    // we're clear to leave this stop.
                    self.advance_schedule_to_next_stop();
                }
            }
            VehicleState::Travelling(_, to_stop_id) => {
                let planned_stop = &self.schedule[self.route_idx].1.stops[self.stop_idx];
                if self.stop_idx == 0 && self.schedule[self.route_idx].0 > time_s {
                    // this is the first stop on the route, but it's not time to start yet.
                    self.state = VehicleState::Idle(Some(self.schedule[self.route_idx].0));
                } else if self.riders.contains_key(to_stop_id) || 
                          (self.get_room() > 0 && 
                           (planned_stop.await_departure || current_stop_has_queue)) {
                    // we need to stop at this stop.
                    let num_disembarkers = match self.riders.get(to_stop_id) {
                        Some(vec) => vec.len(),
                        None => 0, 
                    };
                    let done_disembark_time = time_s + 
                        (num_disembarkers as f64 * disembark_period_s) as u32;
                    let done_time = if planned_stop.await_departure {
                        let dep = planned_stop.get_departure_offset_s();
                        let arr = planned_stop.get_arrival_offset_s();
                        let scheduled_wait = dep - arr;
                        let scheduled_departure = time_s + scheduled_wait;  
                        std::cmp::max(scheduled_departure, done_disembark_time)
                    } else {
                        done_disembark_time
                    };
                    self.state = VehicleState::AtStopOnRoute(done_time, to_stop_id.clone());
                } else {
                    // we don't need to stop here.
                    self.advance_schedule_to_next_stop();
                }
            }

            VehicleState::Idle(_) => {
                let planned_stop = &self.schedule[self.route_idx].1.stops[self.stop_idx];
                if planned_stop.await_departure || current_stop_has_queue {
                    let stop_id = planned_stop.facility_id.clone();
                        // we need to wait for the departure
                    let dep_allowed_time = if planned_stop.await_departure {
                        // we can leave only when the departure time has passed.
                        let dep_offset_s = planned_stop.get_departure_offset_s();
                        time_s + dep_offset_s
                    } else {
                        // we're allowed to depart as soon as we're done boarding.
                        time_s + 1
                    };
                    self.state = VehicleState::AtStopOnRoute(dep_allowed_time, stop_id);
                } else {
                    self.advance_schedule_to_next_stop();
                }
            }
        };

        if old_state != self.state {
            // update the log of the vehicle's real journey.
            if let Some(leg) = self.real_journey.last_mut() {
                leg.end_time_s = Some(time_s); 
            }
            let skip = if let VehicleState::Travelling(from_id, to_id) = &self.state {
                // don't record dummy "travelling" states between the same stop
                from_id == to_id
            } else {
                false
            };
            if ! skip {
                
                let new_leg = VehicleLeg {
                    state: self.state.clone(),
                    start_time_s: time_s,
                    end_time_s: None,
                    route_id: match self.get_current_route_id() {
                        None => None,
                        Some(id) => Some(id.clone()),
                    },
                    departure_id: match self.get_current_departure_id() {
                        None => None,
                        Some(id) => Some(String::from(id)),
                    }
                };
                self.real_journey.push(new_leg);
            }
        }
    }
}


fn parse_vehicles_xml(xml_string: &str)
                      -> (HashMap<String, String>,
                          HashMap<String, PtVehicleType>) {

    // initialize the vehicle collections
    let mut vehicles = HashMap::new();
    let mut vehicle_types = HashMap::new();
    let mut parser = EventReader::new(xml_string.as_bytes());

    // let mut parser = config_utils::xml_parser_from_path(path);


    while let ee = parser.next() {

        // check for errors or the end of the document.
        if let Err(err) = ee {
            log::error!("Error: {}", err);
            break;
        } else if let Ok(XmlEvent::EndDocument) = ee {
            log::info!("Reached end of vehicles xml");
            break;
        }

        // we're safe if we get here, so unwrap the xml event.
        let ee = ee.unwrap();
        if let XmlEvent::StartElement{ name, attributes, .. } = &ee {
            if name.local_name == "vehicle" {
                // store the string data for the vehicle, then create it
                // later when we know we've got all the vehicle types.
                let id = config_utils::get_xml_attribute_value(&attributes, "id")
                    .expect("Xml vehicle has no ID!");
                let type_id = config_utils::get_xml_attribute_value(&attributes, "type")
                    .expect("Xml vehicle has no vehicle type!");
                vehicles.insert(id, type_id);

            } else if name.local_name == "vehicleType" {

                // collect the elements pertaining to this vehicle type
                let mut vt_xml_events = vec![ee];
                while let vte = parser.next() {
                    match vte {
                        Ok(XmlEvent::EndElement { name })
                            if name.local_name == "vehicleType" => break,
                        Ok(other) => vt_xml_events.push(other),
                        Err(_) => break,
                    }
                }

                // build the vehicle type from the xml elements
                let veh_type = PtVehicleType::from_xml_events(vt_xml_events);
                vehicle_types.insert(veh_type.id.clone(), veh_type);
            }
        }
    }

    (vehicles, vehicle_types)
}


fn parse_schedule_xml(xml_string: &str)
                      -> (HashMap<UniqueRouteId, PtRoute>,
                          HashMap<String, StopFacility>) {
    let mut routes = HashMap::new();
    let mut stop_facilities = HashMap::new();
    let mut parser = EventReader::new(xml_string.as_bytes());

    let mut line_id = String::new();
    let mut curr_route: Option<PtRoute> = None;

    while let ee = parser.next() { match ee {
        // check for errors or the end of the document.
        Err(err) => {
            log::error!("Error: {}", err);
            break;
        }
        Ok(XmlEvent::EndDocument) => {
            log::info!("Reached end of schedule xml");
            break;
        }
        Ok(XmlEvent::EndElement {name}) => {
            // add curr_route to the collection
            if name.local_name == "transitRoute" {
                if let Some(route) = curr_route.take() {
                    // let route_key = route.id;
                    routes.insert(route.id.clone(), route);
                }
            }
        }
        Ok(XmlEvent::StartElement {name, attributes, ..}) => { match name.local_name.as_str() {
            "stopFacility" => {
                // add the stop facility
                let xx = config_utils::get_xml_attribute_value(&attributes, "x")
                    .unwrap()
                    .parse()
                    .unwrap();
                let yy = config_utils::get_xml_attribute_value(&attributes, "y")
                    .unwrap()
                    .parse()
                    .unwrap();
                let id = config_utils::get_xml_attribute_value(&attributes, "id")
                    .unwrap();
                let link = config_utils::get_xml_attribute_value(&attributes, "linkRefId")
                    .unwrap();

                let sf = StopFacility {
                    id: id.clone(),
                    link: link,
                    pos: Point2d::new(xx, yy),
                };
                stop_facilities.insert(id.clone(), sf);
            }

            "transitLine" => {
                line_id = config_utils::get_xml_attribute_value(&attributes, "id").unwrap();
            }

            "transitRoute" => {
                // collect the routes on this line
                let route_id = config_utils::get_xml_attribute_value(&attributes, "id")
                    .unwrap();

                curr_route = Some(PtRoute {
                    id: UniqueRouteId {
                        line_id: String::from(&line_id),
                        route_id,
                    },
                    mode: String::new(),
                    stops: vec![],
                    links: vec![],
                    departures: vec![],
                });
            }

            "transportMode" => {
                // get the next element, it should contain the text.
                match parser.next() {
                    Ok(XmlEvent::Characters(content)) => {
                        if let Some(route) = curr_route.as_mut() {
                            route.mode = String::from(content);
                        }
                    }
                    _ => panic!("transportMode element contained no text!"),
                }
            }

            "stop" => {
                // Add the planned stop to the current route
                if let Some(route) = curr_route.as_mut() {
                    let arr_offset = match config_utils::get_xml_attribute_value(&attributes,
                                                                              "arrivalOffset")
                    {
                        Some(val) => Some(config_utils::get_num_seconds_from_time_str(&val)),
                        None => None,
                    };

                    let dep_offset = match config_utils::get_xml_attribute_value(&attributes,
                                                                              "departureOffset")
                    {
                        Some(val) => Some(config_utils::get_num_seconds_from_time_str(&val)),
                        None => None,
                    };

                    let facility_id = config_utils::get_xml_attribute_value(&attributes,
                                                              "refId").unwrap();

                    let await_dep = match config_utils::get_xml_attribute_value(&attributes,
                                                                             "awaitDeparture") {
                        Some(boolval) => boolval.parse().unwrap(),
                        // if not provided, take 'true' by default.
                        None => true,                                                                                
                    };
                    
                    let stop = PtPlannedStop::new(facility_id, arr_offset, dep_offset, await_dep);
                    route.stops.push(stop);
                }
            },

            "link" => {
                if let Some(route) = curr_route.as_mut() {
                    let link_id = config_utils::get_xml_attribute_value(&attributes,
                                                                     "refId").unwrap();
                    route.links.push(link_id);
                }
            },

            "departure" => {
                if let Some(route) = curr_route.as_mut() {
                    let id = config_utils::get_xml_attribute_value(&attributes,
                                                                "id").unwrap();

                    let vehicle_id = config_utils::get_xml_attribute_value(&attributes,
                                                                        "vehicleRefId")
                                                                        .unwrap();

                    let dt = config_utils::get_xml_attribute_value(&attributes,
                                                                "departureTime").unwrap();
                    let start_time_s = config_utils::get_num_seconds_from_time_str(&dt);

                    let dep = Departure {
                        id,
                        start_time_s,
                        vehicle_id,
                    };

                    route.departures.push(dep);
                }
            },
            _ => {}
        }},
        _ => {}
    }}

    (routes, stop_facilities)
}


#[cfg(test)]
mod tests {

    use crate::test_utils;

    use super::*;
    use super::super::{PtLeg, JourneyLeg};

    static TEST_VEHICLE_XML: &str = r###"
    <?xml version='1.0' encoding='ASCII'?>
    <vehicleDefinitions xmlns="http://www.matsim.org/files/dtd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.matsim.org/files/dtd http://www.matsim.org/files/dtd/vehicleDefinitions_v1.0.xsd">
      <vehicleType id="1">
        <description>large bus--avg_power_kW=26.7--mode=road</description>
        <capacity>
          <seats persons="50"/>
          <standingRoom persons="100"/>
        </capacity>
        <length meter="12"/>
      </vehicleType>
      <vehicleType id="2">
        <description>small bus--avg_power_kW=15.3--mode=road</description>
        <capacity>
          <seats persons="25"/>
          <standingRoom persons="50"/>
        </capacity>
        <length meter="8"/>
      </vehicleType>
      <vehicleType id="3">
        <description>smaller bus--avg_power_kW=12.0--mode=road</description>
        <capacity>
          <seats persons="34"/>
          <standingRoom persons="24"/>
        </capacity>
        <length meter="5"/>
      </vehicleType>
      <vehicle id="0" type="1"/>
      <vehicle id="1" type="2"/>
      <vehicle id="2" type="2"/>
      <vehicle id="3" type="3"/>
      <vehicle id="4" type="3"/>
      <vehicle id="5" type="3"/>
    </vehicleDefinitions>
    "###;

    static TEST_SCHEDULE_XML: &str = r###"
    <?xml version='1.0' encoding='ASCII'?>
    <!DOCTYPE transitSchedule SYSTEM "http://www.matsim.org/files/dtd/transitSchedule_v1.dtd">
    <transitSchedule>
      <transitStops>
        <stopFacility id="0-1" x="5" y="0" linkRefId="0-1l"/>
        <stopFacility id="1-0" x="0" y="0" linkRefId="1-0l"/>
        <stopFacility id="1-2" x="5" y="6" linkRefId="1-2l"/>
        <stopFacility id="2-1" x="5" y="-1" linkRefId="2-1l"/>
        <stopFacility id="0-2" x="5" y="7.0" linkRefId="0-2l"/>
        <stopFacility id="2-0" x="0" y="-1" linkRefId="2-0l"/>
      </transitStops>
      <transitLine id="main">
        <transitRoute id="eastward">
          <transportMode>pt</transportMode>
          <routeProfile>
            <stop refId="0-1" departureOffset="0:00:00" awaitDeparture="true"/>
            <stop refId="1-2" arrivalOffset="0:10:30" awaitDeparture="true"/>
          </routeProfile>
          <route>
            <link refId="0-1"/>
            <link refId="1-2"/>
          </route>
          <departures>
            <departure id="0" departureTime="1:01:00" vehicleRefId="0"/>
            <departure id="1" departureTime="1:02:00" vehicleRefId="1"/>
          </departures>
        </transitRoute>
        <transitRoute id="westward">
          <transportMode>pt</transportMode>
          <routeProfile>
              <stop refId="2-1" departureOffset="0:00:00" awaitDeparture="false"/>
              <stop refId="1-0" arrivalOffset="0:10:30" awaitDeparture="false"/>
          </routeProfile>
          <route>
              <link refId="2-1"/>
              <link refId="1-0"/>
          </route>
          <departures>
            <departure id="0" departureTime="1:01:00" vehicleRefId="2"/>
            <departure id="1" departureTime="1:02:00" vehicleRefId="3"/>
            <departure id="2" departureTime="5:02:00" vehicleRefId="4"/>
          </departures>
        </transitRoute>
      </transitLine>
      <transitLine id="other">
        <transitRoute id="loop">
          <transportMode>train</transportMode>
          <routeProfile>
            <stop refId="0-1" departureOffset="0:00:00" awaitDeparture="true"/>
            <stop refId="1-2" arrivalOffset="0:10:30" departureOffset="0:11:00" awaitDeparture="false"/>
            <stop refId="2-0" arrivalOffset="0:20:30"/>
          </routeProfile>
          <route>
            <link refId="0-1"/>
            <link refId="1-2"/>
            <link refId="2-0"/>
          </route>
          <departures>
            <departure id="0" departureTime="1:01:00" vehicleRefId="5"/>
          </departures>
        </transitRoute>
      </transitLine>
    </transitSchedule>
    "###;


    #[test]
    fn test_vehicles_parsing() -> Result<(), std::io::Error> {
        let mut true_vehicles = HashMap::new();
        true_vehicles.insert("0".to_string(), "1".to_string());
        true_vehicles.insert("1".to_string(), "2".to_string());
        true_vehicles.insert("2".to_string(), "2".to_string());
        true_vehicles.insert("3".to_string(), "3".to_string());
        true_vehicles.insert("4".to_string(), "3".to_string());
        true_vehicles.insert("5".to_string(), "3".to_string());

        let mut true_vehicle_types = HashMap::new();
        true_vehicle_types.insert("1".to_string(), PtVehicleType::new(
            "1", "road", "large bus", 50, 100, 12.0, 26.7));
        true_vehicle_types.insert("2".to_string(), PtVehicleType::new(
            "2", "road", "small bus", 25, 50, 8.0, 15.3,));
        true_vehicle_types.insert("3".to_string(), PtVehicleType::new(
            "3", "road", "smaller bus", 34, 24, 5.0, 12.0));

        let (vehicles, vehicle_types) = parse_vehicles_xml(TEST_VEHICLE_XML);

        // Check there are the right number of vehicles
        test_utils::compare_hashmaps(&vehicles, &true_vehicles);
        test_utils::compare_hashmaps(&vehicle_types, &true_vehicle_types);
        Ok(())
    }

    #[test]
    fn test_schedule_parsing() -> Result<(), std::io::Error> {
        let mut true_routes = HashMap::new();
        let id = UniqueRouteId {
            line_id: "main".to_string(),
            route_id: "eastward".to_string(),
        };
        true_routes.insert(id.clone(),
            PtRoute {
                id: id,
                mode: "pt".to_string(),
                stops: vec![
                    PtPlannedStop::new("0-1".to_string(), None, Some(0), true),
                    PtPlannedStop::new("1-2".to_string(), Some(630), None, true),
                    ],
                links: vec!["0-1".to_string(), "1-2".to_string()],
                departures: vec![
                    Departure {id: "0".to_string(), start_time_s: 3660,
                               vehicle_id: "0".to_string()},
                    Departure {id: "1".to_string(), start_time_s: 3720,
                               vehicle_id: "1".to_string()},
                    ],
            }
        );

        let id = UniqueRouteId {
            line_id: "main".to_string(),
            route_id: "westward".to_string(),
        };
        true_routes.insert(id.clone(),
            PtRoute {
                id: id,
                mode: "pt".to_string(),
                stops: vec![
                    PtPlannedStop::new("2-1".to_string(), None, Some(0), false),
                    PtPlannedStop::new("1-0".to_string(), Some(630), None, false),
                    ],
                links: vec!["2-1".to_string(), "1-0".to_string()],
                departures: vec![
                    Departure {id: "0".to_string(), start_time_s: 3660,
                               vehicle_id: "2".to_string()},
                    Departure {id: "1".to_string(), start_time_s: 3720,
                               vehicle_id: "3".to_string()},
                    Departure {id: "2".to_string(), start_time_s: 18120,
                               vehicle_id: "4".to_string()},
                    ],
            }
        );

        let id = UniqueRouteId {
            line_id: "other".to_string(),
            route_id: "loop".to_string(),
        };
        true_routes.insert(id.clone(),
            PtRoute {
                id: id,
                mode: "train".to_string(),
                stops: vec![
                    PtPlannedStop::new("0-1".to_string(), None, Some(0), true),
                    PtPlannedStop::new("1-2".to_string(), Some(630), Some(660), false),
                    PtPlannedStop::new("2-0".to_string(), Some(1230), None, true),
                    ],
                links: vec!["0-1".to_string(), "1-2".to_string(),
                            "2-0".to_string()],
                departures: vec![
                    Departure {id: "0".to_string(), start_time_s: 3660,
                               vehicle_id: "5".to_string()},
                    ],
            }
        );

        let mut true_stop_facs = HashMap::new();
        true_stop_facs.insert("0-1".to_string(),
                              StopFacility {
                                  id: "0-1".to_string(),
                                  link: "0-1l".to_string(),
                                  pos: Point2d::new(5.0, 0.0),
        });
        true_stop_facs.insert("1-0".to_string(),
                              StopFacility {
                                  id: "1-0".to_string(),
                                  link: "1-0l".to_string(),
                                  pos: Point2d::new(0.0, 0.0),
        });
        true_stop_facs.insert("1-2".to_string(),
                              StopFacility {
                                  id: "1-2".to_string(),
                                  link: "1-2l".to_string(),
                                  pos: Point2d::new(5.0, 6.0),
        });
        true_stop_facs.insert("2-1".to_string(),
                              StopFacility {
                                  id: "2-1".to_string(),
                                  link: "2-1l".to_string(),
                                  pos: Point2d::new(5.0, -1.0),
        });
        true_stop_facs.insert("0-2".to_string(),
                              StopFacility {
                                  id: "0-2".to_string(),
                                  link: "0-2l".to_string(),
                                  pos: Point2d::new(5.0, 7.0),
        });
        true_stop_facs.insert("2-0".to_string(),
                              StopFacility {
                                  id: "2-0".to_string(),
                                  link: "2-0l".to_string(),
                                  pos: Point2d::new(0.0, -1.0),
        });

        let (routes, stop_facs) = parse_schedule_xml(TEST_SCHEDULE_XML);

        test_utils::compare_hashmaps(&routes, &true_routes);
        test_utils::compare_hashmaps(&stop_facs, &true_stop_facs);
        Ok(())
    }

    #[test]
    fn test_capacity_scaling() {
        // capacities to check
        let base_ptsys = PtSystem::from_xml(TEST_VEHICLE_XML, TEST_SCHEDULE_XML);
        let mut ptsys = PtSystem::from_xml(TEST_VEHICLE_XML, TEST_SCHEDULE_XML);
        for scale in [0.0, 0.1, 0.333, 0.5, 0.9, 1.0].iter() {
            ptsys.scale_capacities(*scale);
            for (vtid, base_vtype) in &base_ptsys.vehicle_types {

                let vtype = ptsys.vehicle_types.get(vtid).unwrap();
                let to_compare = [
                    (vtype.scaled_num_seats, base_vtype.num_seats),
                    (vtype.scaled_standing_room, base_vtype.standing_room),
                ];

                for (scaled, base) in to_compare.iter() {
                    let scaled_base = *base as f64 * scale;
                    assert_eq!(scaled_base, *scaled);
                }

                let scaled_base_cap = (base_vtype.get_capacity() as f64 * scale).round() as usize;
                assert_eq!(vtype.get_capacity(), scaled_base_cap);

            }
        }
    }

    #[test]
    #[should_panic]
    fn test_scale_capacity_above_one() {
        // scaling greater than 1 should not be allowed!
        let mut ptsys = PtSystem::from_xml(TEST_VEHICLE_XML, TEST_SCHEDULE_XML);
        ptsys.scale_capacities(1.1);
    }

    #[test]
    fn test_add_rider() {
        let mut ptsys = PtSystem::from_xml(TEST_VEHICLE_XML, TEST_SCHEDULE_XML);
        let (_, vehicle) = build_simple_scenario(&ptsys, 100, "3");
        let base_start_room = vehicle.get_room();

        for scale in [0.0, 0.3, 0.5, 0.66667, 1.0].iter() {
            ptsys.scale_capacities(*scale);
            let (mut trips, mut vehicle) = build_simple_scenario(&ptsys, 100, "3");
    
            let start_room = vehicle.get_room();
            assert_eq!(start_room, (base_start_room as f64 * *scale).round() as usize);
            for ii in 0..start_room {
                if let Some(trip) = trips.pop() {
                    vehicle.add_rider(trip);
                    // check that the room decreases
                    assert_eq!(start_room - (ii + 1), vehicle.get_room());
                } else {
                    assert_eq!(vehicle.get_room(), 0);
                    break;
                }
            }
        }
    }

    #[test]
    #[should_panic]
    fn test_exceed_capacity() {
        let mut ptsys = PtSystem::from_xml(TEST_VEHICLE_XML, TEST_SCHEDULE_XML);
        let scale = 0.1;
        ptsys.scale_capacities(scale);
        // vehicle room should be 6
        let (trips, mut vehicle) = build_simple_scenario(&ptsys, 7, "3");

        for trip in trips {
            vehicle.add_rider(trip);
        }
    }


    fn build_simple_scenario<'a>(ptsys: &'a PtSystem, num_trips: usize, veh_type_id: &str) ->
        (Vec<PassengerTrip>, PtVehicle<'a>) {
        // assemble some trips
        let orig = Point2d::new(0.0, 0.0);
        let dest = Point2d::new(100.0, 100.0);
        let mut trips: Vec<PassengerTrip> = (0..num_trips).map(|idnum| {
            PassengerTrip::new(&idnum.to_string(), idnum as u32, 0, 
                               orig.clone(), dest.clone(), 0, 1.0)
        }).collect();

        let (route_id, route) = ptsys.routes.iter().next().unwrap();

        let journey = vec![
            JourneyLeg::PtLeg(PtLeg{
                route_id: route_id.clone(),
                board_stop_id: String::from("0"),
                disembark_stop_id: String::from("1"),
                board_time_s: 0,
                disembark_time_s: Some(100),
                departure_id: String::from("dep"),
            })
        ];
        for trip in trips.iter_mut() {
            trip.set_planned_journey(journey.clone());
        }

        let schedule = vec![(0, route)];
        let veh_type = ptsys.vehicle_types.get(veh_type_id).unwrap();
        let vehicle = PtVehicle::new("0", veh_type, schedule);
        return (trips, vehicle);
    }
}
