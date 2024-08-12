use rsgen_avro::{Generator, Source};

fn main() {
    let files = std::fs::read_dir("schema/ztf").unwrap();
    for file in files {
        let file = file.unwrap();
        let path = file.path();
        println!("Generating types from schema: {:?}", path);
        
        let raw_schema = std::fs::read_to_string(path).unwrap();
        let source = Source::SchemaStr(&raw_schema);
        let mut out = std::io::stdout();

        let g = Generator::new().unwrap();
        g.gen(&source, &mut out).unwrap();
    }
}