use halcyon::FromRow;

#[derive(FromRow)]
union Bad {
    a: i32,
    b: f32,
}

fn main() {}
