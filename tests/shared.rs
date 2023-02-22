#![allow(clippy::all, dead_code)]
use dotenv::dotenv;
use once_cell::sync::OnceCell;
static CELL: OnceCell<()> = OnceCell::new();
/// call this at top of each integrtion test
pub fn setup_integration() {
    CELL.get_or_init(|| {
        match dotenv() {
            Ok(_) => {}
            Err(_) => {
                eprint!("no .env file detected")
            }
        }
        env_logger::init();
    });
}

/// simple struct that manages creation and cleanup of a directory in the
/// `testdata` folder...
pub struct TestDir {
    pub path: std::path::PathBuf,
}
impl TestDir {
    pub fn as_string(&self) -> String {
        self.path.to_str().unwrap().to_string()
    }
    pub fn new(createdir: bool) -> Self {
        let path = std::path::PathBuf::new()
            .join(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join(format!("_testdir{}", hex::encode(randbytes(4))));
        if createdir {
            std::fs::create_dir(&path).unwrap();
        }
        Self { path }
    }
}

fn randbytes(n: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let b: u8 = rand::random();
        v.push(b);
    }
    v
}

pub fn test_confdir() {
    let _td = TestDir::new(true);
}
