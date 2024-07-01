use ndarray::prelude::*;

#[derive(PartialEq, Debug, Clone)]
pub struct Point2d {
    pub x_coord: f64,
    pub y_coord: f64,
}

impl Point2d {
    pub fn new(x_coord: f64, y_coord: f64) -> Point2d {
        Point2d{x_coord, y_coord}
    }

    pub fn as_array(&self) -> [f64; 2] {
        [self.x_coord, self.y_coord]
    }

    pub fn dot(&self, other: &Point2d) -> f64 {
        self.x_coord * other.x_coord + self.y_coord * other.y_coord
    }

    pub fn plus(&self, other: &Point2d) -> Point2d {
        Point2d::new(self.x_coord + other.x_coord, self.y_coord + other.y_coord)
    }

    pub fn minus(&self, other: &Point2d) -> Point2d {
        Point2d::new(self.x_coord - other.x_coord, self.y_coord - other.y_coord)
    }

    pub fn times(&self, factor: f64) -> Point2d {
        Point2d::new(self.x_coord * factor, self.y_coord * factor)
    }

    pub fn euclidean_distance(&self, other: &Point2d) -> f64 {
        let diff = self.minus(other);
        (diff.x_coord.powi(2) + diff.y_coord.powi(2)).sqrt()
    }

    pub fn nearest_point_on_line(&self, segment: &LineSegment) -> Point2d {
        // v = J - I
        let vv = segment.end_j.minus(&segment.end_i);
        // u = I - P (P being the point represented by self)
        let uu = segment.end_i.minus(self);
        // tt is the fractional distance from I to J where the nearest point to P lies.
        let tt = - vv.dot(&uu) / vv.dot(&vv);
        if tt <= 0.0 {
            // the closest point on the line is before end i of the segment, so the nearest segment point is i.
            return segment.end_i.clone();
        } else if tt >= 1.0 {
            // the closest point on the line is after end j of the segment, so the nearest segment point is j.
            return segment.end_j.clone();
        } else {
            // the closest point is between the ends of the segment
            let i_part = segment.end_i.times(1.0 - tt);
            let j_part = segment.end_j.times(tt);
            return i_part.plus(&j_part);
        }
    }
}


/// If a second vector is provided, computes the distances between all points in the first and all
/// points in the second vector.  Otherwise, computes distances between each pair of points in
/// the first.
pub fn euclidean_distances(points1: &Vec<Point2d>, points2: Option<&Vec<Point2d>>)
    -> Array<f64, Ix2> {
    let mut points_array1 = Array::zeros((points1.len(), 1, 2));
    for (ii, point) in points1.iter().enumerate() {
        points_array1[[ii, 0, 0]] = point.x_coord;
        points_array1[[ii, 0, 1]] = point.y_coord;
    }
    let mut points_array2 = match points2 {
        Some(p2) => {
            let mut points_array2 = Array::zeros((p2.len(), 1, 2));
            for (ii, point) in p2.iter().enumerate() {
                points_array2[[ii, 0, 0]] = point.x_coord;
                points_array2[[ii, 0, 1]] = point.y_coord;
            }
            points_array2
        }
        None => points_array1.clone(),
    };

    // compute differences between each point on each dimension
    points_array2.swap_axes(0, 1);
    let mut dim_dists: Array<f64, Ix3> = points_array1 - points_array2;

    // compute euclidean distances from dimension differences
    dim_dists.par_mapv_inplace(|xx| xx.powi(2));
    let mut dists_mat = dim_dists.sum_axis(Axis(2));
    dists_mat.par_mapv_inplace(|xx| f64::sqrt(xx));
    
    return dists_mat;
}

pub struct LineSegment {
    pub end_i: Point2d,
    pub end_j: Point2d,
}

impl LineSegment {
    // pub fn new(end_i: Point2d, end_j: Point2d) -> LineSegment {
    //     LineSegment {end_i, end_j}
    // }

    pub fn from_refs(end_i: &Point2d, end_j: &Point2d) -> LineSegment {
        LineSegment {
            end_i: end_i.clone(), 
            end_j: end_j.clone()
        }
    }
}