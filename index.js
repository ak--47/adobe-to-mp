/*
----
DEPENDENCIES
----
*/
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const Papa = require('papaparse');
const md5 = require('md5');
const u = require('ak-tools');
const path = require('path');
const os = require('os');
const { Storage } = require('@google-cloud/storage');
const functions = require('@google-cloud/functions-framework');
const { createWriteStream } = require('fs');
const bunyan = require('bunyan');
const { LoggingBunyan } = require('@google-cloud/logging-bunyan');
const loggingBunyan = new LoggingBunyan({ logName: 'adobe-transform', redirectToStdout: true });
const zlib = require('zlib');
const { Transform } = require('stream');
const isLocal = process.env.RUNTIME === "dev";
const RUNTIME = process.env.RUNTIME || "unknown";
const TEMP_DIR = isLocal ? path.resolve("./tmp") : os.tmpdir();
const MB = 25;
const log = bunyan.createLogger({
	name: 'adobe-transform',
	streams: [
		// Log to the console at 'info' and above
		{ stream: process.stdout, level: 'debug' },
		// And log to Cloud Logging, logging at 'info' and above
		loggingBunyan.stream('info'),
	]

});

/*
----
CUSTOMER SUPPLIED LOOKUP TABLES
----
*/

// standard adobe prop values
const lookups = await getLookups(`./lookups-standard/`);
const enumerableLookups = Object.keys(lookups);

// columns for adobe raw TSV file, supplied by customer
const headers = await getHeaders(`./lookups-custom/columns.csv`);

// metric lists to resolve event names (use hashmaps for lookups because they are faster)
const metrics = await getHashMap(`./guides/metrics.csv`, "", 3, 1);
const standardEventList = await getHashMap(`./lookups-custom/eventStandard.tsv`);
// THESE LOOKUPS ARE UNUSED
// const customEventList = await getHashMap(`./lookups-custom/eventList.csv`);
// const evars = await getHashMap(`./guides/evars.csv`, "variables/");
// const props = await getHashMap(`./guides/props.csv`, "variables/");

//whitelists
// const whitelistEvars = await getWhitelist(`./guides/whitelist-evars.csv`);
// const whitelistProps = await getWhitelist(`./guides/whitelist-props.csv`);
// const whitelistMetrics = await getWhitelist(`./guides/whitelist-metrics.csv`);
// const whitelist = [...whitelistEvars, ...whitelistProps, ...whitelistMetrics, "post_product_list", "post_event_list"];



/*
----
CLOUD ENTRY
----
*/

functions.http('start', async (req, res) => {
	try {
		const sourceFile = getFileName(req.body.cloud_path);
		log.info({ file: sourceFile, ...req.body }, "TRANSFORM START");
		const { cloud_path, dest_path } = req.body;
		const { human, delta } = await main(cloud_path, dest_path);
		log.info({ file: sourceFile, elapsed: delta, ...req.body }, `TRANSFORM END: ${human}`);
		res.status(200).send({ status: "OK" });
	} catch (e) {
		log.error({ error: e, body: req.body }, "ERROR!");
		res.status(500).send(e);
	}
});

/*
----
MAIN
----
*/

async function main(cloud_path, dest_path) {
	const timer = u.timer('transform');
	timer.start();

	//cloud storage setup
	const storage = new Storage();
	const { bucket, file: cloudURI } = u.parseGCSUri(cloud_path);
	const filename = path.basename(cloud_path);
	const f = { file: filename };
	const TEMP_FILE_TRANSFORMED = path.basename(cloud_path.replace(".tsv.gz", ".ndjson"));
	const TEMP_FILE_TRANSFORMED_PATH = path.join(TEMP_DIR, TEMP_FILE_TRANSFORMED);

	log.debug(f, 'streaming + transforming');
	const remoteFile = storage.bucket(bucket).file(cloudURI);

	const writeStream = createWriteStream(TEMP_FILE_TRANSFORMED_PATH);
	writeStream.on('error', function (err) {
		log.error(err, "WRITE ERROR!");
	});

	const parseStream = Papa.parse(Papa.NODE_STREAM_INPUT, {
		header: true,
		fastMode: true,
		skipEmptyLines: true,
		transformHeader: (header, index) => headers[index]["Column name"],
		transform: cleanAdobeRaw,
	});
	parseStream.on('error', function (err) {
		log.error(err, "PARSE ERROR!");
	});

	const transformStream = new Transform({
		objectMode: true, // this allows passing objects
		transform(chunk, encoding, callback) {
			const mpEvent = adobeToMixpanel(chunk);
			if (Array.isArray(mpEvent)) {
				for (const event of mpEvent) {
					this.push(JSON.stringify(event) + '\n');
				}
			}

			else {
				this.push(JSON.stringify(mpEvent) + '\n');
			}
			callback();
		}
	});

	transformStream.on('error', function (err) {
		log.error(err, "TRANSFORM ERROR!");
	});


	//pipeline
	await new Promise((resolve, reject) => {
		remoteFile.createReadStream({ highWaterMark: 1024 * 1024 * MB })
			.pipe(zlib.createGunzip({ chunkSize: 1024 * 1024 * MB }))
			.pipe(parseStream)
			.pipe(transformStream)
			.pipe(writeStream)
			.on('finish', async () => {
				writeStream.end();
				resolve();

			})
			.on('error', (err) => {
				writeStream.end();
				reject(err);
			});

	});


	log.debug(f, 'uploading');
	const { file: upload_path } = u.parseGCSUri(dest_path);
	const destination = path.join(upload_path, TEMP_FILE_TRANSFORMED);
	const [uploaded] = await storage.bucket(bucket).upload(TEMP_FILE_TRANSFORMED_PATH, { destination, gzip: true });
	if (!isLocal) {
		await u.rm(TEMP_FILE_TRANSFORMED_PATH);
	}
	timer.stop(false);
	log.debug(f, 'job done');
	return { ...timer.report(false), source: cloud_path, destination: 'gs://'.concat(bucket).concat('/').concat(uploaded.name) };






}

/*
----
TRANSFORMS
----
*/

//transform adobe to mixpanel
function adobeToMixpanel(row) {
	const mixpanelEvent = {
		"event": "hit",
		"properties": {
			"distinct_id": row.mcvisid,
			"time": Number(row.hit_time_gmt) || Number(row.cust_hit_time_gmt) || Number(row.last_hit_time_gmt), //Number(row.cust_hit_time_gmt),
			...u.removeNulls(row)
		}
	};

	if (isNaN(mixpanelEvent.properties.time)) debugger;
	if (!Boolean(mixpanelEvent.properties.time)) debugger;
	if (row['__parsed_extra']) debugger;


	// for (const key in mixpanelEvent.properties) {
	// 	if (!whitelist.includes(key)) {			
	// 		delete mixpanelEvent.properties[key];
	// 	}
	// }


	//use visid_high and visid_low if it's available
	if ((row.visid_high !== "0" && row.visid_high) || (row.visid_low !== "0" && row.visid_low)) {
		mixpanelEvent.properties.distinct_id = `${row.visid_high}${row.visid_low}`;
	}

	// no insert_id
	const hash = md5(`${row?.hitid_high || ""}-${row?.hitid_low || ""}`);

	mixpanelEvent.properties.$insert_id = hash;

	//this is only used for special hits where we need to "explode" the adobe data
	const explodeMatches = ['orders']; //'plp loads', 'checkouts'
	
	if (row.post_event_list?.some(x => explodeMatches?.some(match => x?.toLowerCase() === match))) {
		if (row.post_product_list) {
			const products = row.post_product_list.split(';');
			products.shift(); //remove first element, which is always IGNORED
			
			// if (products.length % 5 !== 0) { 
			// 	debugger;
			// }

			//THE FORMULA TO EXTRACT THE PRODUCT DATA IS AS FOLLOWS:
			/**			 
			 * 0 : product id
			 * 1 : quantity
			 * 2 : total price
			 * 3 : ??? IGNORE
			 * 4 : long description [remove]
			 * 5 : NEXT product id
			 * 6 : NEXT quantity
			 * 7 : NEXT total price
			 * 8 : NEXT ???
			 * 9 : NEXT long description [remove]
			 * 10 : NEXT NEXT product id
			 * 11 : NEXT NEXT quantity
			 * 12 : NEXT NEXT total price
			 * 13 : NEXT NEXT ???
			 * 14 : NEXT NEXT long description [remove]
			 */

			const productChunks = [...chunks(products, 5)];
			const explodedProps = productChunks.map(chunk => {
				const values = {};
				if (chunk[0]) values.product_id = chunk[0]; //this may not exist
				if (chunk[1]) values.quantity = +chunk[1]; // this exists only on orders
				if (chunk[2]) values.total_price = +chunk[2]; // this exists only on orders				
				return values;
			});
			//the event that matched
			mixpanelEvent.properties.MATCHED_EVENT_PRODUCT_HITS = row.post_event_list.filter(x => explodeMatches.some(match => x?.toLowerCase()?.includes(match)))[0];
			mixpanelEvent.properties.PRODUCTS_HITS = [];
			for (const explodedProp of explodedProps) {
				//only valid values product_ids
				if (explodedProp.product_id) {
					//only valid values quantity and total_price
					if (!isNaN(explodedProp.quantity) && !isNaN(explodedProp.total_price)) {
						mixpanelEvent.properties.PRODUCTS_HITS.push(explodedProp);
					}
					
				}
			}
		}
	}

	return mixpanelEvent;
}

// resolve row values to human readable values
function cleanAdobeRaw(value, header) {
	//set "" to null
	if (value === "") return null;
	//set "--" to null
	if (value === "--" || value === '-') return null;
	//set ":" to null
	if (value === ":") return null;

	//standard adobe dimensions
	if (enumerableLookups.includes(header?.toLowerCase())) {
		value = lookups[header.toLowerCase()].get(value);
	}

	//nested json objects
	if (isJSON(value)) {
		try {
			value = JSON.parse(value);
		}

		catch (e) {
			//note: adobe truncates json objects to 1000 characters, so this is a common error and there's nothing we can do about it
			value = null;
		}
	}

	//post_event_list is where we define events; a "hit" is multiple events
	if (header === "post_event_list") {
		const events = value.split(',').map(a => a.trim());
		const eventNames = events.map(event => {
			//some events are like 704=20... where 704 is the custom event id and 20 is the duration
			if (event.includes("=")) {
				event = event.split("=")[0];
			}

			//resolving metrics from metrics.csv
			if (metrics.get(event)) return metrics.get(event);

			//resolve standard events from eventStandard.csv, although this should almost never happen
			else if (standardEventList.get(event)) return standardEventList.get(event);

			//if we can't resolve the event name, return it's number
			else {
				return event;
			}
		});

		return eventNames.filter(a => a);
	}

	return value;
}


/*
----
HELPERS
----
*/


function isJSON(string) {
	if (typeof string !== 'string') return false;
	if (string.startsWith('{')) {
		return true;
	}
	else {
		return false;
	}
};

function getFileName(cloud_path) {
	const { bucket, file: cloudURI } = u.parseGCSUri(cloud_path);
	const filename = path.basename(cloud_path);
	return filename;
}

async function getLookups(standardLookupsFolder) {
	const standardLookups = await u.ls(path.resolve(standardLookupsFolder));
	const results = {};
	for (const lookup of standardLookups) {
		const lookupName = path.basename(lookup, '.csv').replace(".tsv", "");
		const rawFile = await u.load(lookup);
		const lookupData = Papa.parse(rawFile, { header: false }).data;
		const lookupMap = new Map(lookupData.map(i => [i[0], i[1]]));
		results[lookupName] = lookupMap;
	}
	return results;
}

async function getHashMap(customLookupsFile, replacePhrase, keyCol = 0, ValueCol = 1) {
	const rawFile = await u.load(customLookupsFile);
	const parsedFile = Papa.parse(rawFile, { header: false }).data;
	const lookup = new Map(parsedFile.map(i => {
		if (replacePhrase) return [i[keyCol].toString().replace(replacePhrase, "").toLowerCase(), i[ValueCol]];
		return [i[keyCol], i[ValueCol]];
	}));
	return lookup;
}

async function getWhitelist(file, column = 0, separator = "/") {
	const rawFile = await u.load(file);
	const parsedFile = Papa.parse(rawFile, { header: false }).data;
	const whitelist = parsedFile.map(i => i[column].split(separator)[1]).filter(a => a);
	return whitelist;
}

async function getHeaders(headersFile) {
	const rawFile = await u.load(headersFile);
	const parsedFile = Papa.parse(rawFile, { header: true }).data;
	return parsedFile;
}

function* chunks(arr, n) {
	for (let i = 0; i < arr.length; i += n) {
		yield arr.slice(i, i + n);
	}
}

export default main;