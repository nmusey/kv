use clap::{Parser, Subcommand};
use sqlite::{Connection, State};

#[derive(Parser)]
#[command(version, about)]
struct Args {
    #[command(subcommand)]
    command: Commands,

}

#[derive(Subcommand)]
enum Commands {
    #[command(alias="g")]
    Get { 
        key: String,

        #[arg(long, short='t')]
        time: bool,
    },
    #[command(alias="s")]
    Set { 
        key: String, 
        value: String 
    },
    #[command(alias="all", alias="a")]
    History { 
        key: String,
        
        #[arg(long, short='t')]
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
        Commands::Set { key, value } => set_key(&ctx, key, value),
        Commands::History { key, time } => get_history(&ctx, key, time),
    }
}

fn make_context() -> Context {
    return Context {
        conn: sqlite::open("kv.db").expect("Could not open connection to database"),
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

    let mut prepared = ctx.conn.prepare(query).unwrap();
    while let Ok(State::Row) = prepared.next()
    {
        let value = prepared.read::<String, _>("v").unwrap();
        let created = prepared.read::<String, _>("created").unwrap();

        if time {
            println!("{} {}", created, value);
        } else {
            println!("{}", value);
        }
    }
}

fn set_key(ctx: &Context, key: String, value: String) {
    let query = format!("INSERT INTO kv(k, v) VALUES ('{}', '{}');", key, value);
    match ctx.conn.execute(query) {
        Err(e) => {
            println!("Could not insert key into database: {}", e)
        }
        _ => {}
    }
}

fn get_history(ctx: &Context, key: String, time: bool) {
    let query = format!("SELECT v, created FROM kv WHERE k='{}' ORDER BY created ASC;", key);
    let mut prepared = ctx.conn.prepare(query).unwrap();
    while let Ok(State::Row) = prepared.next()
    {
        let value = prepared.read::<String, _>("v").unwrap();
        let created = prepared.read::<String, _>("created").unwrap();

        if time {
            println!("{} {}", created, value);
        } else {
            println!("{}", value);
        }
    }
}
