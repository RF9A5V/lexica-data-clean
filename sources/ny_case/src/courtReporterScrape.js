// Used to pull clusters that contain refs to dockets and opinions for NY Court of Appeals
// Should generalize

import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

const COURT_LISTENER_API_KEY = process.env.COURT_LISTENER_API_KEY;
if (!COURT_LISTENER_API_KEY) {
  throw new Error('COURT_LISTENER_API_KEY is not set in environment variables. Please set it in your .env file.');
}

const courtListenerUrl = "https://www.courtlistener.com/api/rest/v4";
const courtListenerHeaders = {
  "Authorization": `Token ${COURT_LISTENER_API_KEY}`,
  "Content-Type": "application/json"
};

async function fetchCourtListenerData() {
  const response = await fetch(`${courtListenerUrl}/dockets/?court=ny`, {
    method: "GET",
    headers: courtListenerHeaders
  });
  const data = await response.json();
  return data;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function pullCourtListenerClusters(url, stream, count = { total: 0 }) {
  const maxRetries = 5;
  let attempt = 0;
  let data;
  while (attempt < maxRetries) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: courtListenerHeaders
      });
      if (!response.ok) {
        throw new Error(`[HTTP ${response.status}] ${response.statusText}`);
      }
      data = await response.json();
      break; // Success
    } catch (err) {
      attempt++;
      const delay = Math.min(16000, 1000 * Math.pow(2, attempt - 1)); // 1s, 2s, 4s, 8s, 16s
      console.error(`[RETRY] Attempt ${attempt} failed for ${url}:`, err.message || err);
      if (attempt >= maxRetries) {
        console.error(`[ERROR] Giving up after ${maxRetries} attempts:`, url);
        return;
      }
      console.log(`[RETRY] Waiting ${delay / 1000}s before next attempt...`);
      await sleep(delay);
    }
  }

  for (let cluster of data.results) {
    stream.write(JSON.stringify(cluster) + '\n');
    count.total++;
    if (count.total % 100 === 0) {
      console.log(`[DEBUG] Written ${count.total} opinions.`);
    }
  }

  if (data.next) {
    console.log(`[DEBUG] Next page: ${data.next}`);

    // Write next URL to separate file for continuation
    const nextUrlFilePath = path.join(__dirname, 'next_url.txt');
    fs.writeFileSync(nextUrlFilePath, data.next);

    await sleep(1500);
    await pullCourtListenerClusters(data.next, stream, count);
  }
}

let startURL = "https://www.courtlistener.com/api/rest/v4/clusters/?docket__court=ny&precedential_status=Published&fields=case_name,case_name_full,citations,date_created,date_filed,date_filed_is_approximate,date_modified,sub_opinions";

let nextUrlFilePath = path.join(__dirname, 'next_url.txt');
let nextUrl = fs.readFileSync(nextUrlFilePath, 'utf8');

(async () => {
  const filePath = path.join(__dirname, 'clusters.ndjson');
  const stream = fs.createWriteStream(filePath, { flags: 'w' });


  let count = { total: 0 };
  // Read number of lines from filepath
  try {
    const data = fs.readFileSync(filePath, 'utf8');
    const lines = data.split('\n');
    count.total = lines.length;
    console.log(`[DEBUG] Found ${count.total} existing opinions.`);
  } catch (err) {
    console.error('[ERROR]', err);
  }

  try {
    await pullCourtListenerClusters(nextUrl || startURL, stream, count);
    console.log('[DEBUG] Done streaming all clusters.');
  } catch (err) {
    console.error('[ERROR]', err);
  } finally {
    stream.end();
  }
})();

// fetchCourtListenerData().then(async data => {
//   // Write to file
//   if(data.next) {
//     await fetchCourtListenerData(data.next);
//   }
//   const filePath = path.join(__dirname, 'dockets.json');
//   fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
// });