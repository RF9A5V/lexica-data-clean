// Used this to generate a report on the number of opinions per reporter
// Probably don't need later

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import ndjson from "ndjson";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });


const filePath = path.join(__dirname, 'clusters.ndjson');
const fileStream = fs.createReadStream(filePath);
const stream = fileStream.pipe(ndjson.parse());

let reporterCount = {};

for await (const cluster of stream) {
  let { citations } = cluster;
  for (let citation of citations) {
    let { reporter } = citation;
    if (!reporterCount[reporter]) {
      reporterCount[reporter] = 0;
    }
    reporterCount[reporter]++;
  }
}

let sortedReporters = Object.entries(reporterCount).sort((a, b) => b[1] - a[1]);

console.log(sortedReporters);
  

