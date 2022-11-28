/**
 * This is an ETL app that takes as input a bzip2 encoded JSON Wikidata dump,
 * streams it through a decoder, extracts the desirable fields, and outputs
 * the result
 *
 * THINGS TO NOTE: in Rust, strings are UTF8 encoded (meaning a given character
 * can be anywhere from 1 to 4 bytes).
 */

use std::cmp::min;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Write, BufWriter};
use std::path::{PathBuf};
use std::time::{Instant};
use bzip2::read::{MultiBzDecoder};
use clap::{Parser};
use futures_util::StreamExt;
use indicatif::{HumanDuration, ProgressBar, ProgressStyle, HumanBytes};
use log::{debug, info};
use simdutf8::basic::from_utf8;
use reqwest;

// Must be large enough to hold the largest entry
const BUFFER_LENGTH: usize = 500000;

#[derive(Parser, Debug)]
#[clap(author="alexgagnon", version, about="Download and filter wikidata dumps")]
struct Cli {
    #[clap(short = 'c', long = "continue-on-error", help = "Don't bail on error while filtering")]
    continue_on_error: bool,

    #[clap(short = 'd', long = "download", help = "Download wikidata dump json file (default is to '.')")]
    download: bool,

    #[clap(parse(from_os_str), short = 'i', long = "input", required = false, takes_value = true, required = false, help = "Source wikidata dump source")]
    input_file_path: Option<PathBuf>,

    #[clap(parse(from_os_str), short = 'o', long = "output", help = "Filename to output filtered entities (default is stdout)")]
    output_file_path: Option<PathBuf>,

    #[clap(short = 'f', long = "force", help = "Force overwriting files")]
    force_overwrite: bool,   

    #[clap(short = 'j', long = "jq-filter", default_value = "", help = "jq filter, see https://stedolan.github.io/jq/ for usage. NOTE: The filter is applied to EACH ENTITY!")]
    jq_filter: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    debug!("Starting...");

    let args = Cli::parse();
    debug!("{:?}", args);
    
    if args.download {
        let start = Instant::now();
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
        
        let mut file = {
            let filename = res
                .url()
                .path_segments()
                .and_then(|segments| segments.last())
                .and_then(|name| if name.is_empty() { None } else { Some(name) })
                .unwrap();
    
            let filename = env::current_dir()?.join(filename);
            info!("Downloading to {:?}", filename.as_os_str());
            File::create(filename)?
        };

        let pb = ProgressBar::new(total_size);
        pb.set_style(ProgressStyle::default_bar()
            .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
            .progress_chars("#>-"));
            
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

        pb.finish_with_message(format!("Downloaded {} to {:?} in {}", &url, file, HumanDuration(start.elapsed())));
    }

    if !args.jq_filter.is_empty() {
        let mut output: Box<dyn Write>;
        if args.output_file_path.is_none() {
            let stdout = std::io::stdout(); // get the global stdout entity
            output = Box::new(stdout.lock()) as Box<dyn Write>; // acquire a lock on it
        }
        else {
            if args.output_file_path.clone().unwrap().exists() && !args.force_overwrite {
                panic!("Output file already exists, must use `force-overwrite` flag to continue");
            }
            // TODO: handle gracefully
            let output_file = File::create(args.output_file_path.unwrap());
            output = Box::new(output_file?) as Box<dyn Write>;
        }

        process(args.input_file_path, &mut output, &args.jq_filter, args.continue_on_error)?;
    }
    else {
        info!("No filter provided");
    }
    
    Ok(())
}

pub fn process(input: Option<PathBuf>, output: &mut impl Write, jq_filter: &String, continue_on_error: bool) -> Result<(), std::io::Error> {
    let mut stream = BufWriter::new(output);
    let input = File::open(input.unwrap())?;
    let mut filter = jq_rs::compile(jq_filter).unwrap();

    let size = input.metadata()?.len();
    debug!("Opening {:?}, size: {}", input, size);

    let mut total_bytes: u64 = 0;

    debug!("Initializing buffer to size {}", BUFFER_LENGTH);
    let reader = BufReader::new(input);
    let mut md = MultiBzDecoder::new(reader);

    let mut buffer = [0; BUFFER_LENGTH];
    let mut str_buffer = String::new();

    let bar = ProgressBar::new(size);

    bar.set_draw_rate(1);
    bar.set_style(ProgressStyle::default_bar()
    .template("[{elapsed_precise}] Processed {bytes}, {msg} entities")
    .progress_chars("#>-"));

    // discard the first two bytes representing "[\n"
    // NOTE both of these are ASCII characters, so one byte each
    md.read(&mut [0u8; 2])?;

    let mut num_entities = 0;
    let mut n = md.read(&mut buffer)?;
    
    let start = Instant::now();
    let default = "null".to_string();

    while n > 0 {
        total_bytes += n as u64;
        bar.inc(n as u64);
        // convert to string and split on newlines
        str_buffer.push_str(&from_utf8(&buffer[..n]).unwrap());
        let mut entities: Vec<&str> = str_buffer.split(",\n").collect();

        // for each "complete" entities (i.e. terminated with  comma and newline), filter and output
        for entity in &entities[..(entities.len())] {
            num_entities += 1;
            debug!("{}", entity);
            let result = &filter.run(entity);
            let filtered_entity = match result {
                Ok(e) => e,
                Err(error) => if !continue_on_error {panic!("Could not parse: {}. {}", entity, error)} else {&default}
            };
            debug!("{}", filtered_entity);
            stream.write_all(&filtered_entity.as_bytes())?;
            bar.set_message(format!("{}", num_entities));
        }
        
        // reset the string buffer with the incomplete last entity
        str_buffer = last.to_string();

        // create a new empty buffer
        buffer = [0; BUFFER_LENGTH];
        n = md.read(&mut buffer)?;
    }

    bar.finish_with_message(format!("Finished! Processed {}, with {} entities in {}", HumanBytes(total_bytes), num_entities + 1, HumanDuration(start.elapsed())));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process() {
        let input = std::path::Path::new("./tests/invalid-json.json.bz2").to_path_buf();
        process(Some(input), &mut std::io::stdout(), &".id".to_string(), true);
    }
}