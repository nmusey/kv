use clap::{Parser, Subcommand};
use sqlite::{Connection, State};
use std::env;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(alias = "g")]
    Get {
        key: String,

        #[arg(long, short = 't')]
        time: bool,
    },

    #[command(alias = "s")]
    Set { 
        key: String, 

        #[arg(num_args=1..)]
        values: Vec<String>
    },

    #[command(alias = "all", alias = "a")]
    History {
        key: String,

        #[arg(long, short = 't')]
        time: bool,
    },
}

struct Context {
    conn: Connection,
}

fn main() {
    let ctx = make_context();
    setup_tables(&ctx);

    let args = Args::parse();
    match args.command {
        Commands::Get { key, time } => get_key(&ctx, key, time),
        Commands::Set { key, values } => set_key(&ctx, key, values),
        Commands::History { key, time } => get_history(&ctx, key, time),
    }
}

fn make_context() -> Context {
    let db_file = match env::var("KV_PATH") {
        Ok(path) => path,
        Err(_) => "/tmp/kv.db".to_string(),
    };

    return Context {
        conn: sqlite::open(db_file).expect("Could not open connection to database"),
    };
}

fn setup_tables(ctx: &Context) {
    let query = "CREATE TABLE IF NOT EXISTS kv(
        id INTEGER PRIMARY KEY,
        k TEXT, 
        v TEXT,
        created DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS k_index ON kv(k);
    ";

    ctx.conn.execute(query).unwrap();
}

fn get_key(ctx: &Context, key: String, time: bool) {
    let query = format!(
        "SELECT v, created FROM kv WHERE k='{}' ORDER BY created DESC LIMIT 1;",
        key
    );

    print_values(query, ctx, time);
}

fn set_key(ctx: &Context, key: String, values: Vec<String>) {
    let query = format!("INSERT INTO kv(k, v) VALUES ('{}', '{}');", key, values.join(" "));
    match ctx.conn.execute(query) {
        Err(e) => {
            println!("Could not insert key into database: {}", e)
        }
        _ => {}
    }
}

fn get_history(ctx:  &Context, key: String, time: bool) {
    let query = format!(
        "SELECT v, created FROM kv WHERE k='{}' ORDER BY created ASC;",
        key
    );

    print_values(query, ctx, time);
}

fn print_values(query: String, ctx: &Context, time: bool) {
    let mut prepared = ctx.conn.prepare(query).unwrap();
    while let Ok(State::Row) = prepared.next() {
        let value = prepared.read::<String, _>("v").unwrap();
        let created = prepared.read::<String, _>("created").unwrap();

        match time {
            true => println!("{} {}", created, value),
            false => println!("{}", value)
        }
    }
}
