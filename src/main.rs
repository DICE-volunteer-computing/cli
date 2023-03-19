use mongodb::bson::doc;
use rust_sdk::model::{
    artifact::{ArtifactType, CreateArtifactDTO, Status as ArtifactStatus, UpdateArtifactDTO},
    entity::EntityType,
    job::CreateJobDTO,
    job_execution::{CreateJobExecutionDTO, Status as JobExecutionStatus},
    project::CreateProjectDTO,
    runtime::{CreateRuntimeDTO, Status as RuntimeStatus, UpdateRuntimeDTO},
};
use std::{
    collections::HashMap,
    env,
    ffi::OsStr,
    fs::{self, File},
    io::{self, Cursor, Read},
    path::PathBuf,
    process::Command,
};

use clap::Parser;

/// DICE Command Line Interface
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Arguments {
    /// Action to create and upload a runtime to DICE
    #[arg(short, long)]
    create_runtime: bool,

    /// Action to create and upload an input artifact
    #[arg(short, long)]
    create_input_artifact: bool,

    /// Action to create a new project
    #[arg(short, long)]
    create_project: bool,

    /// Action to create a new job
    #[arg(short, long)]
    create_job: bool,

    /// Action to create a new job execution
    #[arg(short, long)]
    create_job_execution: bool,

    /// Action to get an existing job execution
    #[arg(short, long)]
    get_job_execution: bool,

    /// List pending notifications
    #[arg(short, long)]
    list_notifications: bool,

    /// Download output artifacts for a job execution into the current directory
    #[arg(short, long)]
    download_output_artifacts: bool,

    /// Name (optional for some commands, required for others)
    #[arg(short, long)]
    name: Option<String>,

    /// Description (optional for some commands, required for others)
    #[arg(short, long)]
    description: Option<String>,

    /// Project ID (optional for some commands, required for others)
    #[arg(short, long)]
    project_id: Option<String>,

    /// Job ID (optional for some commands, required for others)
    #[arg(short, long)]
    job_id: Option<String>,

    /// Job execution ID (optional for some commands, required for others)
    #[arg(short, long)]
    job_execution_id: Option<String>,

    /// Runtime ID (optional for some commands, required for others)
    #[arg(short, long)]
    runtime_id: Option<String>,

    /// File (optional for some commands, required for others)
    #[arg(short, long, use_value_delimiter = true, value_delimiter = ',')]
    input_artifact_ids: Option<Vec<String>>,

    /// File (optional for some commands, required for others)
    #[arg(short, long)]
    file: Option<String>,
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

async fn create_runtime(name: String, project_id: String) {
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
            Ok(_) => {
                println!("Successfully uploaded runtime");

                // Set runtime status to active
                rust_sdk::api::runtime::update(
                    create_runtime_response.id.clone(),
                    UpdateRuntimeDTO {
                        status: RuntimeStatus::Active,
                    },
                )
                .await;

                println!("Created runtime: {}", create_runtime_response.id);
            }
            Err(err) => println!("Could not upload runtime: {}", err),
        };
    } else {
        println!("NOT IN A DICE RUNTIME");
    }
}

async fn create_input_artifact(project_id: String, file_name: String) {
    let tar_file_name = format!("{}.tar", file_name);

    // Compress the file
    Command::new("tar")
        .arg("-czf")
        .arg(tar_file_name.clone())
        .arg(file_name)
        .status()
        .expect("Could not tar the input artifact");

    // Utilizing the rust SDK, get an upload link
    let create_artifact_response = rust_sdk::api::artifact::create(CreateArtifactDTO {
        entity_id: project_id,
        entity_type: EntityType::Project,
        artifact_type: ArtifactType::Input,
        tags: HashMap::new(),
    })
    .await;

    // Load runtime file
    let mut file = File::open(tar_file_name.clone()).expect("Could not open tar file");

    // Read the file contents into a buffer
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .expect("Could not read tar file");

    // Upload the compressed file
    let upload_response = reqwest::Client::new()
        .put(create_artifact_response.uri)
        .body(buffer)
        .send()
        .await;
    match upload_response {
        Ok(_) => {
            println!("Successfully uploaded input artifact");

            //  Delete tar file
            Command::new("rm")
                .arg(tar_file_name)
                .status()
                .expect("Could not delete tar file");

            // Set input artifact status to active
            rust_sdk::api::artifact::update(
                create_artifact_response.id.clone(),
                UpdateArtifactDTO {
                    status: ArtifactStatus::Active,
                },
            )
            .await;

            println!("Created input artifact: {}", create_artifact_response.id);
        }
        Err(err) => println!("Could not upload input artifact: {}", err),
    };
}

async fn create_project(description: String) {
    // Utilizing the rust SDK, create a project
    let project_id = rust_sdk::api::project::create(CreateProjectDTO {
        description: description,
        tags: HashMap::new(),
    })
    .await;

    println!("Created project: {}", project_id);
}

async fn create_job(project_id: String, runtime_id: String, input_artifact_ids: Vec<String>) {
    // Utilizing the rust SDK, create a job
    let create_job_response = rust_sdk::api::job::create(CreateJobDTO {
        project_id: project_id,
        runtime_id: runtime_id,
        input_artifact_ids: input_artifact_ids,
        tags: HashMap::new(),
    })
    .await;

    println!("Created job: {}", create_job_response.id);
}

async fn create_job_execution(job_id: String) {
    // Utilizing the rust SDK, create a job execution
    let create_job_execution_response =
        rust_sdk::api::job_execution::create(CreateJobExecutionDTO {
            job_id: job_id,
            tags: HashMap::new(),
        })
        .await;

    println!(
        "Created job execution: {}",
        create_job_execution_response.id
    );
}

async fn get_job_execution(job_execution_id: String) {
    // Utilizing the rust SDK, get an existing job execution
    let job_execution = rust_sdk::api::job_execution::get(job_execution_id).await;

    println!("Job execution: {:?}", job_execution);
}

async fn download_output_artifacts(job_execution_id: String) {
    // Utilizing the rust SDK, get an existing job execution
    let job_execution = rust_sdk::api::job_execution::get(job_execution_id).await;

    if job_execution.status != JobExecutionStatus::Completed {
        panic!("Cannot download artifacts for a job that has not yet been completed");
    }

    // Create directory for job
    let job_root_path = job_execution.id.to_string();
    fs::create_dir_all(job_root_path.clone()).expect("Could not create job output directory");

    // Change directory into directory
    env::set_current_dir(job_root_path).expect("Could not change directories");

    // Get list of output artifacts for job execution
    let artifacts = rust_sdk::api::artifact::list(doc! {
        "artifact_type": serde_json::to_string(&ArtifactType::Output).unwrap().replace("\"", ""),
        "entity_id": job_execution.id,
        "status": serde_json::to_string(&ArtifactStatus::Active).unwrap().replace("\"", "")
    })
    .await;

    // For each artifact in job execution, download it, untar it, and then remove the tar file
    let task_handles = artifacts.into_iter().map(|artifact| {
        tokio::spawn(async move {
            //  Download artifact
            let tar_file_path = format!("{}.tar", artifact.id.to_string());

            let download_artifact_response =
                rust_sdk::api::artifact::download(artifact.id.to_string()).await;
            let response = reqwest::get(download_artifact_response.uri).await.unwrap();

            let mut artifact_file = File::create(&tar_file_path).unwrap();
            let mut content = Cursor::new(response.bytes().await.unwrap());
            std::io::copy(&mut content, &mut artifact_file)
                .expect("Could not copy artifact to file");

            //  Untar the artifact
            Command::new("tar")
                .arg("-xvf")
                .arg(tar_file_path.clone())
                .status()
                .expect("Could not untar the output artifact");

            //  Delete tar file
            Command::new("rm")
                .arg(tar_file_path)
                .status()
                .expect("Could not delete tar file");
        })
    });

    for handler in task_handles {
        handler.await.expect("Could not upload ouput artifact");
    }
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();

    if args.create_runtime {
        create_runtime(
            args.name.expect("--name required"),
            args.project_id.expect("--project-id required"),
        )
        .await;
    } else if args.create_input_artifact {
        create_input_artifact(
            args.project_id.expect("--project-id required"),
            args.file.expect("--file required"),
        )
        .await;
    } else if args.create_project {
        create_project(args.description.expect("--description required")).await;
    } else if args.create_job {
        create_job(
            args.project_id.expect("--project-id required"),
            args.runtime_id.expect("--runtime-id required"),
            args.input_artifact_ids
                .expect("--input-artifact-ids required"),
        )
        .await;
    } else if args.create_job_execution {
        create_job_execution(args.job_id.expect("--job-id required")).await;
    } else if args.get_job_execution {
        get_job_execution(args.job_execution_id.expect("--job-execution-id required")).await;
    } else if args.download_output_artifacts {
        download_output_artifacts(args.job_execution_id.expect("--job-execution-id required"))
            .await;
    }
}
