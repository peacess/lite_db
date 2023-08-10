use std::{fs, path};

pub fn file_name(file: &str, name: &str, ex: &str) -> String {
    let file_name = format!("temp/{}/{}.{}", file, name, ex);
    let db_path = path::Path::new(&file_name);
    if !db_path.exists() {
        fs::create_dir_all(db_path).expect("");
    }
    file_name
}

pub fn path_name(file: &str, name: &str) -> String {
    let path_name = format!("temp/{}/{}", file, name);
    let path_buf = path::PathBuf::from(path_name.clone());
    if !path_buf.exists() {
        fs::create_dir_all(path_buf).expect("");
    }
    path_name
}