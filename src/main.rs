/**
 * This is an ETL app that takes as input a bzip2 encoded JSON Wikidata dump,
 * streams it through a decoder, extracts the desirable fields, and outputs
 * the result
 *
 * THINGS TO NOTE: in Rust, strings are UTF8 encoded (meaning a given character
 * can be anywhere from 1 to 4 bytes).
 */

use std::cmp::min;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{PathBuf};
use std::time::{Instant};
use bzip2::read::{MultiBzDecoder};
use clap::Parser;
use futures_util::StreamExt;
use indicatif::{HumanDuration, ProgressBar, ProgressStyle, HumanBytes};
use log::{debug};
use simdutf8::basic::from_utf8;
use reqwest;

const BUFFER_LENGTH: usize = 500000;

#[derive(Parser, Debug)]
#[clap(author="alexgagnon", version, about="Download and convert wikidata json dump to filtered ndjson")]
struct Cli {
    #[clap(parse(from_os_str), short = 'f', long = "file", required = false, takes_value = true, required = false, help = "Source wikidata dump source")]
    input_file_path: Option<PathBuf>,

    #[clap(short = 'd', long = "download", help = "Download wikidata dump json file")]
    download_latest: bool,

    #[clap(parse(from_os_str), short = 'o', long = "output", default_value = "./wikidata-dump.ndjson", help = "Filename to output filtered entities")]
    output_file_path: PathBuf,

    #[clap(short = 'j', long = "jq-filter", default_value = "", help = "jq filter, see https://stedolan.github.io/jq/ for usage. NOTE: The filter is applied to EACH ENTITY!")]
    jq_filter: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    debug!("Starting...");
    let args = Cli::parse();
    debug!("{:?}", args);

    let size: u64;

    debug!("Initializing buffer to size {}", BUFFER_LENGTH);
    let mut buffer = [0; BUFFER_LENGTH];
    let mut str_buffer = String::new();

    let start = Instant::now();

    let input_file_path = if args.input_file_path.is_none() {tempfile::Builder::new().prefix("wikidata-dump.bz2").tempdir()?.into_path()} else {args.input_file_path.unwrap()};
    let mut output_file = File::create(args.output_file_path)?;
    let mut filter = jq_rs::compile(&args.jq_filter).unwrap();

    if args.download_latest {
        let version = "latest".to_string();
        let url = &format!("https://dumps.wikimedia.org/wikidatawiki/entities/{}-all.json.bz2", version).to_owned();
        debug!("URL: {}", url);
        let res = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .or(Err(format!("Failed to GET from '{}'", &url)))?;
        let total_size = res
            .content_length()
            .ok_or(format!("Failed to get content length from '{}'", &url))?;

        let pb = ProgressBar::new(total_size);
        pb.set_style(ProgressStyle::default_bar()
            .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
            .progress_chars("#>-"));
        pb.set_message(format!("Downloading {} to {:?}", url, input_file_path));

        let mut file = File::create(&input_file_path).or(Err(format!("Failed to create file '{:?}'", input_file_path)))?;
        let mut downloaded: u64 = 0;
        let mut stream = res.bytes_stream();

        while let Some(item) = stream.next().await {
            let chunk = item.or(Err(format!("Error while downloading file")))?;
            file.write_all(&chunk)
                .or(Err(format!("Error while writing to file")))?;
            let new = min(downloaded + (chunk.len() as u64), total_size);
            downloaded = new;
            pb.set_position(new);
        }

        pb.finish_with_message(format!("Downloaded {} to {:?}", &url, input_file_path));
    }

    if !args.jq_filter.is_empty() {
        let input_file = File::open(input_file_path)?;
        let mut total_bytes: u64 = 0;
        debug!("Opening {:?}", input_file);
        size = input_file.metadata()?.len();
        let reader = BufReader::new(input_file);
        let mut md = MultiBzDecoder::new(reader);

        let bar = ProgressBar::new(size);
        bar.set_draw_rate(2);
        bar.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] Processed {bytes}, {msg} entities")
        .progress_chars("##-"));

        // discard the first two bytes representing "[\n"
        // NOTE both of these are ASCII characters, so one byte each
        md.read(&mut [0u8; 2])?;

        let mut num_entities = 0;
        let mut n = md.read(&mut buffer)?;
        
        while n > 0 {
            total_bytes += n as u64;
            bar.inc(n as u64);
            // convert to string and split on newlines
            str_buffer.push_str(&from_utf8(&buffer[..n]).unwrap());
            let entities: Vec<&str> = str_buffer.split(",\n").collect();

            // for each "complete" entities (i.e. terminated with newline), filter and output
            for entity in &entities[..(entities.len() - 1)] {
                num_entities += 1;
                let filtered_entity = &filter.run(entity).unwrap();
                debug!("-----");
                debug!("Original: {:?}", entity);
                debug!("Filtered: {:?}", filtered_entity);
                output_file.write_all(filtered_entity.as_bytes())?;
                bar.set_message(format!("{num_entities}"));
            }

            // reset the string buffer with the (incomplete) last entity
            str_buffer = entities.last().unwrap().clone().to_owned();

            // create a new empty buffer
            buffer = [0; BUFFER_LENGTH];
            n = md.read(&mut buffer)?;
        }

        bar.finish_with_message(format!("Finished! Processed {} with {} entities in {}", HumanBytes(total_bytes), num_entities, HumanDuration(start.elapsed())));
    }

    Ok(())
}
