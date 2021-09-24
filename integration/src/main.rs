use rjobs::Schedulable;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Log {
    message: String,
}

#[rjobs::job]
impl Schedulable for Log {
    async fn perform(&mut self) -> Result<(), rjobs::Error> {
        println!("log! {}", self.message);
        Ok(())
    }
}

fn main() {}
