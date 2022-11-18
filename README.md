# Wikidata Preprocess

CLI which enables fetching and processing bzip2 compressed wikidata dumps for further use. It uses the `jq` tool under the hood to filter entities and extract desired properties.

NOTE: although `jq` is primarily used to process JSON input, it can output in any format, such as

## Example usage

- `preprocess --download` - Downloads most recent json dump to a temp directory
- `preprocess --download --file ./example.json.bz2` - Downloads most recent json dump to `./example.json.bz2`
- `preprocess --file ./example.json.bz2 --jq-filter "."` - Converts the bz2 compressed json array as-is into decompressed ndjson format
- `preprocess --file ./example.json.bz2 --output ./example.csv --jq-filter '[(.id|ltrimstr("Q")|tonumber), .labels.en.value] | @csv'` - Converts the bz2 compressed json array in decompressed csv with format: `<id>,<label>`
- `preprocess --file ./example.json.bz2 --output ./example.ndjson --jq-filter 'select((.type == "item") and (.labels | has("en")) and (.claims.P31 | map(select(.)))) | [(.id|ltrimstr("Q")|tonumber), .labels.en.value, (.aliases | if has("en") then (.en | map(.value)) else empty end)] | flatten'` - Converts the bz2 compressed json array in decompressed ndjson for only entities with english labels with format: `[<id>,<label>,<aliases...>]`
- `'select((.type == "item") and (.labels | has("en")) and ((.claims.P31 // []) | map(select(.mainsnak.datavalue.value.id == "Q13442814")) | any | not)) | [(.id|ltrimstr("Q")|tonumber), .labels.en.value, (.aliases | if has("en") then (.en | map(.value)) else empty end)] | flatten'` - Same as above, but excludes entities that are instances of (P31) scholarly articles (Q13442814) (NOTE: these take up ~30% of all entries in Wikidata)

You can test jq filters here: https://jqplay.org/
