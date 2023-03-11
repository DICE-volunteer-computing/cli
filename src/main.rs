use rust_sdk::model::runtime::{CreateRuntimeDTO, Status, UpdateRuntimeDTO};
use std::{
    collections::HashMap,
    env,
    ffi::OsStr,
    fs::{self, File},
    io::{self, Read},
    path::PathBuf,
    process::Command,
};

use clap::Parser;

/// DICE Command Line Interface
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Arguments {
    /// Action to upload this runtime to DICE
    #[arg(short, long)]
    upload_runtime: bool,

    /// Name of entity
    #[arg(short, long, default_value_t = format!("N/A"))]
    name: String,

    /// Associated project ID (optional for some commands, required for others)
    #[arg(short, long, default_value_t = format!("N/A"))]
    project_id: String,
}

fn list_files_in_dir(root: &str) -> io::Result<Vec<PathBuf>> {
    let mut result = vec![];

    for path in fs::read_dir(root)? {
        result.push(path?.path().to_owned());
    }

    Ok(result)
}

fn is_directory_dice_runtime(root: &str) -> bool {
    let mut result = false;

    match list_files_in_dir(root) {
        Ok(files) => files.into_iter().for_each(|path| {
            if path.to_str().unwrap().contains(".dice") {
                result = true;
            };
        }),
        Err(_) => (),
    };

    result
}

fn get_current_dir() -> String {
    let cwd: PathBuf = env::current_dir().unwrap();
    let name: &OsStr = cwd.file_name().unwrap();

    name.to_string_lossy().into_owned()
}

async fn upload_runtime(name: String, project_id: String) {
    // Validate that I am in a DICE runtime repository
    if is_directory_dice_runtime(".") {
        println!("Validated located in DICE runtime");

        // Build the runtime
        Command::new("make")
            .arg("clean")
            .status()
            .expect("Could not clean runtime");

        Command::new("make")
            .arg("build")
            .status()
            .expect("Could not build runtime");
        println!("Runtime build completed");

        // Utilizing the rust-sdk, get an upload link
        let create_runtime_response = rust_sdk::api::runtime::create(CreateRuntimeDTO {
            name: name,
            project_id: project_id,
            tags: HashMap::new(),
        })
        .await;

        // Load runtime file
        let mut file = File::open(format!(
            "target/wasm32-wasi/release/{}.tar",
            get_current_dir()
        ))
        .expect("Could not open runtime file");

        // Read the file contents into a buffer
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .expect("Could not read runtime file");

        // Upload the runtime to DICE
        let upload_response = reqwest::Client::new()
            .put(create_runtime_response.uri)
            .body(buffer)
            .send()
            .await;
        match upload_response {
            Ok(_) => println!("Successfully uploaded runtime"),
            Err(err) => println!("Could not upload runtime: {}", err),
        };

        // Set runtime status to active
        rust_sdk::api::runtime::update(
            create_runtime_response.id.clone(),
            UpdateRuntimeDTO {
                status: Status::Active,
            },
        )
        .await;

        println!("Created runtime: {}", create_runtime_response.id);
    } else {
        println!("NOT IN A DICE RUNTIME");
    }
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();

    if args.upload_runtime {
        println!("Uploading runtime");

        upload_runtime(args.name, args.project_id).await;
    }
}
