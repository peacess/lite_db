use std::path::PathBuf;

pub fn file_name(file: &str, name: &str, ex: &str) -> PathBuf {
    let file = file.replace(".", "_");
    let path_buf = PathBuf::from(file).join(format!("{}.{}", name, ex));
    path_buf
}

pub fn path_name(file: &str, name: &str) -> PathBuf {
    let file = file.replace(".", "_");
    let path_buf = PathBuf::from(file).join(name);
    path_buf
}
