use std::{cmp::Reverse, env, io, path::{Path, PathBuf}};
use clap::Parser;

use comfy_table::{Cell, Table};
use dotenv::dotenv;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::{fs, time::Instant};

fn get_env_var_or_exit(name: &str) -> String {
    match std::env::var(name) {
        Ok(val) => val,
        Err(_) => {
            println!("Required variable not set in environment: {name}");
            std::process::exit(1);
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = String::from("./"))]
    target: String,
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let database_url = get_env_var_or_exit("DATABASE_URL");

    let pg_pool = match PgPoolOptions::new()
        .max_connections(100)
        .connect(&database_url)
        .await
    {
        Ok(pool) => {
            println!("Successfully connected to the database.");
            pool
        }
        Err(err) => {
            println!("An error occurred connecting to the database: {err}");
            std::process::exit(1);
        }
    };

    println!("Running queries...");

    let current_dir = env::current_dir().unwrap();
    let mut res_vec = traverse_dirs(pg_pool, &current_dir).await;

    if res_vec.is_empty() {
        println!("No queries found in directory.");
        return;
    }

    res_vec.sort_by_key(|i| Reverse(i.1));

    let mut table = Table::new();
    table.set_header(vec!["Query", "Duration (sec)", "Duration (ms)"]);

    for el in res_vec {
        table.add_row(vec![
            Cell::new(el.0).fg(comfy_table::Color::Blue),
            Cell::new((el.1 as f64) / 1000.0).fg(comfy_table::Color::Green),
            Cell::new(el.1).fg(comfy_table::Color::Green),
        ]);
    }

    println!("{table}");
}

async fn traverse_dirs(pg_pool: PgPool, dir: &Path) -> Vec<(String, u128)> {
    //TODO:Remove unwraps
    let mut stack = vec![dir.to_path_buf()];
    let mut res_vec: Vec<(String, u128)> = vec![];

    while let Some(current_dir) = stack.pop() {
        let mut entries = fs::read_dir(&current_dir).await.unwrap();

        while let Some(entry) = entries.next_entry().await.unwrap() {
            let path = entry.path();
            let file_type = entry.file_type().await.unwrap();

            if file_type.is_dir() {
                stack.push(path);
            } else {
                let filename = entry.file_name().to_str().unwrap_or("").to_string();
                
                if !filename.ends_with(".sql") {
                    continue
                }

                let query: String = fs::read_to_string(entry.path()).await.unwrap();
                res_vec.push(execute_queries_in_file(&pg_pool, filename, query).await);
            }
        }
    }

    res_vec
}

async fn execute_queries_in_file(pg_pool: &PgPool, file_name: String, file_content: String) -> (String, u128) {
    let query_start_time = Instant::now();

    return match sqlx::query(&file_content).fetch_all(pg_pool).await {
        Ok(_) => {
            let elapsed_time = query_start_time.elapsed();
            let query_execution_time_ms = elapsed_time.as_millis();
            (
                file_name,
                query_execution_time_ms,
            )
        }
        Err(_) => { (file_name, 0)}
    }
}
