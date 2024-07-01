use std::path::Path;
use std::collections::HashMap;
use std::fs::File;
use std::error::Error;

// mod geometry;
use super::geometry::Point2d;
use super::pt_system::UniqueRouteId;
use super::Connection;

#[derive(PartialEq, Debug, Clone)]
pub struct WalkingLeg {
    pub start_pos: Point2d,
    pub end_pos: Point2d,
    pub start_time_s: u32,
    pub end_time_s: u32,
}

#[derive(PartialEq, Debug, Clone)]
pub struct PtLeg {
    pub route_id: UniqueRouteId,
    pub board_stop_id: String,
    pub disembark_stop_id: String,
    pub board_time_s: u32,
    pub disembark_time_s: Option<u32>,
    pub departure_id: String,
}

impl PtLeg {
    /// Build a PtLeg from the first and last connections taken on a route
    pub fn from_connections(start: &Connection, end: &Connection) -> PtLeg {
        PtLeg {
            route_id: start.route_id.clone(),
            board_stop_id: start.dep_stop_id.clone(),
            disembark_stop_id: end.arr_stop_id.clone(),
            board_time_s: start.dep_time_s,
            disembark_time_s: Some(end.arr_time_s),
            departure_id: start.departure_id.clone(),
        }
    }
}

// This enum allows a list to hold elements of both PtLeg and WalkingLeg.
#[derive(PartialEq, Debug, Clone)]
pub enum JourneyLeg {
    WalkingLeg(WalkingLeg),
    PtLeg(PtLeg),
}

#[derive(PartialEq, Debug, Clone)]
pub struct PassengerTrip {
    pub id: String,
    // household and member number together indicate a unique person...pre-expansion.
    household_number: u32,
    household_member_number: u32,
    pub origin: Point2d,
    pub destination: Point2d,
    pub planned_start_time_s: u32,
    pub expansion_factor: f64,
    planned_journey: Vec<JourneyLeg>,
    real_journey: Vec<JourneyLeg>,
    pub leg_idx: usize
}

// A convenience type for parsing csv data
type Row = HashMap<String, String>;

impl PassengerTrip {
    pub fn new(id: &str, household_number: u32, household_member_number: u32, origin: Point2d,
               destination: Point2d, planned_start_time_s: u32, expansion_factor: f64)
               -> PassengerTrip
    {
        PassengerTrip {
            id: String::from(id),
            household_number,
            household_member_number,
            origin,
            destination,
            planned_start_time_s,
            expansion_factor,
            planned_journey: vec![],
            real_journey: vec![],
            leg_idx: 0
        }
    }

    pub fn all_from_csv(csvpath: &Path, expand: bool, time_range_start: Option<u32>,
                        time_range_end: Option<u32>)
                        -> Result<Vec<PassengerTrip>, Box<dyn Error>> {
        let file = File::open(csvpath)?;
        let mut reader = csv::Reader::from_reader(file);
        let mut trips = vec![];
        for result in reader.deserialize() {
            let row: Row = result?;
            // some rows don't have t_time specified.  Ignore these rows.
            let t_time: f64 = match row["t_time"].parse() {
                Ok(t_time) => t_time,
                Err(_) => continue
            };
            let start_hours = t_time as u32 / 100;
            let start_minutes = t_time as u32 % 100;
            let start_seconds = ((t_time % 1.) * 60.).round() as u32;
            let start_time_s = start_hours * 3600 + start_minutes * 60 + start_seconds;

            if let Some(trs) = time_range_start {
                if start_time_s < trs {
                    continue;
                }
            }

            if let Some(tre) = time_range_end {
                if tre < start_time_s {
                    continue;
                }
            }

            let trip = PassengerTrip::new(&row["ID"], row["h_id"].parse()?, row["p_rank"].parse()?,
                Point2d::new(row["t_orix"].parse()?, row["t_oriy"].parse()?),
                Point2d::new(row["t_desx"].parse()?, row["t_desy"].parse()?),
                start_time_s, row["t_expf"].parse()?
            );

            if expand {
                let expf = row["t_expf"].parse()?;
                let ids = PassengerTrip::ids_from_expansion_factor(&row["ID"], expf);
                for id in ids {
                    let mut ct = trip.clone();
                    ct.id = id;
                    trips.push(ct);
                }
            } else {
                trips.push(trip);
            }
        }

        Ok(trips)
    }

    pub fn ids_from_expansion_factor(base_id: &str, expf: f64) -> Vec<String> {
        let mut ids = vec![];
        // round the expansion factor to the nearest integer, and use that many copies
        let num_copies = expf.round() as u32;
        if num_copies == 1 {
            ids.push(String::from(base_id));
        } else if num_copies > 1 {
            for ii in 0..num_copies {
                ids.push(format!("{}_exp{}", base_id, ii));
            }
        }

        ids
    }

    pub fn has_plan(&self) -> bool {
        self.planned_journey.len() > 0
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn is_finished(&self) -> bool {
        if self.real_journey.len() == self.planned_journey.len() {
            if let JourneyLeg::PtLeg(ptleg) = self.real_journey.last().unwrap() {
                if let None = ptleg.disembark_time_s {
                    return false;
                }
            } else {
                return true;
            }
        }
        false
    }

    /// Remove all plans and records of actual journeys.
    pub fn clear(&mut self) {
        self.planned_journey.clear();
        self.real_journey.clear();
    }

    pub fn get_planned_journey(&self) -> &Vec<JourneyLeg> {
        &self.planned_journey
    }

    pub fn set_planned_journey(&mut self, journey: Vec<JourneyLeg>) {
        self.planned_journey = journey;
    }

    pub fn get_planned_ptlegs(&self) -> Vec<&PtLeg> {
        get_ptlegs(&self.planned_journey)
    }

    pub fn get_next_planned_ptleg(&self) -> Option<&PtLeg> {
        if self.is_finished() {
            return None;
        }

        let search_leg_idx = self.leg_idx;
        let mut retval = None;
        // get the next stop at which the trip will disembark
        while search_leg_idx < self.planned_journey.len() {
            retval = match &self.planned_journey[search_leg_idx] {
                JourneyLeg::PtLeg(ptleg) => Some(ptleg),
                JourneyLeg::WalkingLeg(_) => None,
            };

            if let Some(_) = retval {
                break;
            }
        }

        retval
    }

    pub fn get_current_planned_leg(&self) -> &JourneyLeg {
        &self.planned_journey[self.leg_idx]
    }

    pub fn get_real_journey(&self) -> &Vec<JourneyLeg> {
        &self.real_journey
    } 

    pub fn get_real_ptlegs(&self) -> Vec<&PtLeg> {
        get_ptlegs(&self.real_journey)
    }

    pub fn get_time_arrived_at_stop(&self) -> Option<u32> {
        if self.real_journey.len() == 0 {
            return None;
        }

        let get_leg_end_time = |leg: &JourneyLeg| {
            match leg {
                JourneyLeg::WalkingLeg(wl) => Some(wl.end_time_s),
                JourneyLeg::PtLeg(ptl) => ptl.disembark_time_s,
            }
        };

        let real_leg_idx = self.real_journey.len() - 1;
        match get_leg_end_time(&self.real_journey[real_leg_idx]) {
            Some(time) => Some(time),
            None if real_leg_idx > 0 => get_leg_end_time(&self.real_journey[real_leg_idx - 1]),
            _ => None,
        }
    }

    pub fn get_current_real_leg(&self) -> &JourneyLeg {
        &self.real_journey[self.leg_idx]
    }

    pub fn get_next_stop_walk_duration(&self) -> u32 {
        match &self.planned_journey[self.leg_idx] {
            JourneyLeg::PtLeg(_) => 0,
            JourneyLeg::WalkingLeg(wl) => wl.end_time_s - wl.start_time_s,
        }
    }

    /// take the first walk if one is needed, and return the time at which it ends.
    pub fn start_journey(&mut self) -> u32 {
        self.take_walk_if_needed(None);
        // return the time at which this walk ends
        match self.real_journey.last().unwrap() {
            JourneyLeg::WalkingLeg(wl) => wl.end_time_s,
            _ => panic!("First leg should always be walking, nobody lives in a bus stop!"),
        }
    }

    fn take_walk_if_needed(&mut self, start_time_s: Option<u32>) {
        if self.is_finished() {
            return;
        }

        let mut next_walk = match &self.planned_journey[self.leg_idx] {
            JourneyLeg::WalkingLeg(wl) => wl.clone(),
            _ => return,
        };

        // If a start time is provided, change the times appropriately
        if let Some(inner_time_s) = start_time_s {
            let duration = next_walk.end_time_s - next_walk.start_time_s;
            next_walk.start_time_s = inner_time_s;
            next_walk.end_time_s = inner_time_s + duration;
        }
        self.real_journey.push(JourneyLeg::WalkingLeg(next_walk));
        self.leg_idx += 1;

    }

    pub fn board(&mut self, time_s: u32, departure_id: &str) {
        match &self.planned_journey[self.leg_idx] {
            JourneyLeg::PtLeg(ptleg) => {
                let mut real_leg = ptleg.clone();
                real_leg.board_time_s = time_s;
                real_leg.disembark_time_s = None;
                real_leg.departure_id = String::from(departure_id);
                self.real_journey.push(JourneyLeg::PtLeg(real_leg));
            },
            JourneyLeg::WalkingLeg(_) => {
                panic!("A boarding passenger should never be on a walking leg!");
            },
        }
    }

    pub fn disembark(&mut self, time_s: u32) {
        match &mut self.real_journey[self.leg_idx] {
            JourneyLeg::PtLeg(ptleg) => {
                ptleg.disembark_time_s = Some(time_s);
                self.leg_idx += 1;
                self.take_walk_if_needed(Some(time_s));
            },
            JourneyLeg::WalkingLeg(_) => {
                panic!("A disembarking passenger should never be on a walking leg!");
            },
        }
    }
}

pub fn get_ptlegs(journey: &Vec<JourneyLeg>) -> Vec<&PtLeg> {
    let mut output = vec![];
    for leg in journey {
        if let JourneyLeg::PtLeg(ptleg) = leg {
            output.push(ptleg);
        }
    }

    output
}


#[cfg(test)]
mod tests {

    use tempfile::tempdir;
    use std::io::Write;
    use super::*;

    #[test]
    fn test_trip_parsing() -> Result<(), Box<dyn Error>> {
        // create a test csv file in a temp directory
        let test_csv = r#"ID,h_id,h_head,h_expf,h_auto,p_rank,nonsense,t_time,t_orix,t_oriy,t_desx,t_desy,t_expf,p_age
0,0,doesn't matter,null,nope,0,nonsense,830,0.5321,-935.323,55.8,100,5.3,50
1,3,doesn't matter,null,nope,2,nonsense,2350,89,0.5,-55.8,-2311,1,23"#;
        let dir = tempdir()?;
        let file_path = dir.path().join("test_trips.csv");
        {
            let mut file = File::create(&file_path)?;
            file.write_all(test_csv.as_bytes())?;
        }

        // parse it to a vector of passenger trips
        let trips = PassengerTrip::all_from_csv(&file_path, false, None, None)?;

        let true_trips = vec![
            PassengerTrip::new("0", 0, 0, Point2d::new(0.5321, -935.323),
                Point2d::new(55.8, 100.0), 30600, 5.3),
            PassengerTrip::new("1", 3, 2, Point2d::new(89.0, 0.5), Point2d::new(-55.8, -2311.0),
                85800, 1.0),
        ];

        // check the contents of the trips
        assert_eq!(true_trips.len(), trips.len());
        for (tt, pt) in true_trips.iter().zip(trips.iter()) {
            assert_eq!(tt, pt);
        }

        // now try with expansion
        let trips = PassengerTrip::all_from_csv(&file_path, true, None, None)?;
        let mut true_trips = vec![
            true_trips[0].clone(),
            true_trips[0].clone(),
            true_trips[0].clone(),
            true_trips[0].clone(),
            true_trips[0].clone(),
            true_trips[1].clone(),
            ];

        // modify the IDs of the cloned true trips
        for ii in 0..5 {
            true_trips[ii].id = format!("{}_exp{}", true_trips[ii].id, ii);
        }

        // check the contents of the trips again.
        assert_eq!(true_trips.len(), trips.len());
        for (tt, pt) in true_trips.iter().zip(trips.iter()) {
            assert_eq!(tt, pt);
        }

        Ok(())
    }
}
