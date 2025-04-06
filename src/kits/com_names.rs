use std::path::PathBuf;

pub fn file_name(file: &str, name: &str, ex: &str) -> PathBuf {
    let file = file.replace(".", "_");
    PathBuf::from(file).join(format!("{}.{}", name, ex))
}

pub fn path_name(file: &str, name: &str) -> PathBuf {
    let file = file.replace(".", "_");
    PathBuf::from(file).join(name)
}
