import { getTokenizedText } from "./parser.js";
import wordBasedFSM from "./wordBasedFSM.js";

async function main() {
    const output = await wordBasedFSM("nysenate:pen:section:10.00");
    console.log(JSON.stringify(output, null, 2))
}

main()