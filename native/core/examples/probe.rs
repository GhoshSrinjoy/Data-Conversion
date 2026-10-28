use dataconv_core::chunk::scan_end;
use dataconv_core::tokenize::Dialect;

fn main() {
    let mut data = b"1,\"x,y,\"\"z\"\"\n".to_vec();
    for _ in 0..20000 {
        data.extend_from_slice(b"x,y,\"\"z\"\"\n");
    }
    let d = Dialect::default();
    for len in [64usize, 65, 128, 1000, 4096, 100_000, data.len()] {
        let len = len.min(data.len());
        println!("{len:>7}: {:?}", scan_end(&data[..len], &d, b'\n'));
    }
}
