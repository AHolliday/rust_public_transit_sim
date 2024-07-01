use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;

use xml::reader::EventReader;
use xml::attribute::OwnedAttribute;


pub fn xml_parser_from_path(path: &Path) -> EventReader<BufReader<File>> {
    let file = File::open(path).unwrap();
    let file = BufReader::new(file);
    EventReader::new(file)
}

pub fn get_xml_attribute_value(attributes: &Vec<OwnedAttribute>, attr_name: &str)
                               -> Option<String> {
    match attributes.iter().find(|attr| attr.name.local_name == attr_name) {
        Some(attr) => Some(attr.value.clone()),
        None => None,
    }
}

pub fn str_to_absolute_path(path_str: &str, default_base_dir: &Path) -> PathBuf {
    let path = PathBuf::from(path_str);
    if path.is_absolute() {
        return path;
    } else {
        return [default_base_dir, Path::new(&path)].iter().collect();
    }
}

pub fn get_num_seconds_from_time_str(timestr: &str) -> u32 {
    let parts: Vec<&str> = timestr.split(":").collect();
    let hours: u32 = parts[0].parse().unwrap();
    let minutes: u32 = parts[1].parse().unwrap();
    let seconds: u32 = parts[2].parse().unwrap();
    hours * 3600 + minutes * 60 + seconds
}
