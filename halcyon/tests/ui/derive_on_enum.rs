use halcyon::FromRow;

#[derive(FromRow)]
enum Bad {
    A,
    B,
}

fn main() {}
