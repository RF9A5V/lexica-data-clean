import { getTokenizedText } from "./parser.js";

async function main() {
    const output = await getTokenizedText("nysenate:abc:section:3");
    console.log(JSON.stringify(output, null, 2))
}

main()