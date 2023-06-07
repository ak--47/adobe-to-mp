import { createRequire } from 'module';
const require = createRequire(import.meta.url);

const Papa = require('papaparse');
const md5 = require('md5');
const u = require('ak-tools');

const path = require('path');
const os = require('os');
const isLocal = process.env.RUNTIME === "dev";
const TEMP_DIR = isLocal ? path.resolve("./tmp") : os.tmpdir();
const highWaterMark = 50000000; //50MB
const mime = require('mime-types');

const lookups = await getLookups(`./lookups-standard/`);
const enumerableLookups = Object.keys(lookups);

const headers = await getHeaders(`./lookups-custom/columns.csv`);
const metrics = await getHashMap(`./guides/metrics.csv`, "", 3, 1);
const standardEventList = await getHashMap(`./lookups-custom/eventStandard.tsv`);
// UNUSED
// const customEventList = await getHashMap(`./lookups-custom/eventList.csv`);
// const evars = await getHashMap(`./guides/evars.csv`, "variables/");
// const props = await getHashMap(`./guides/props.csv`, "variables/");

const { Storage } = require('@google-cloud/storage');
const functions = require('@google-cloud/functions-framework');
const gz = require("node-gzip");
const fs = require('fs/promises');
const { createReadStream, createWriteStream } = require('fs');
const bunyan = require('bunyan');
const { LoggingBunyan } = require('@google-cloud/logging-bunyan');
const loggingBunyan = new LoggingBunyan({ logName: 'adobe-transform' });
const log = bunyan.createLogger({
	name: 'adobe-transform',
	streams: [
		// Log to the console at 'info' and above
		{ stream: process.stdout, level: 'debug' },
		// And log to Cloud Logging, logging at 'info' and above
		loggingBunyan.stream('info'),
	]

});

functions.http('start', async (req, res) => {
	try {
		const sourceFile = getFileName(req.body.cloud_path);
		log.warn({file: sourceFile, ...req.body}, "TRANSFORM START");
		const { cloud_path, dest_path } = req.body;
		const { human, delta } = await main(cloud_path, dest_path);
		log.warn({ file: sourceFile, elapsed: delta, ...req.body }, `TRANSFORM END: ${human}`);
		res.status(200).send({ status: "OK" });
	} catch (e) {
		log.error(e, "ERROR!");
		res.status(500).send(e);
	}
});

async function main(cloud_path, dest_path) {	
	const timer = u.timer('transform');
	timer.start();
	
	//cloud storage setup
	const storage = new Storage();
	const { bucket, file: cloudURI } = u.parseGCSUri(cloud_path);
	const filename = path.basename(cloud_path);
	const f = { file: filename };
	const downloadFile = path.join(TEMP_DIR, filename);
	
	log.debug(f, 'downloading file');
	await storage.bucket(bucket).file(cloudURI).download({ destination: downloadFile, decompress: true });
	const uncompressedFilename = filename.replace(".gz", "");
	const TEMP_FILE_PATH = path.join(TEMP_DIR, uncompressedFilename);
	
	log.debug(f, 'reading file');
	const data = await fs.readFile(downloadFile);
	
	log.debug(f, 'decompressing file');
	const gunzipped = await gz.ungzip(data);
	
	log.debug(f, 'writing decompressed file');
	await u.touch(TEMP_FILE_PATH, gunzipped);
	await u.rm(downloadFile);

	log.debug(f, 'transform decompressed file + write to disk');
	const tsvStream = createReadStream(TEMP_FILE_PATH, { highWaterMark });
	const TEMP_FILE_TRANSFORMED = path.basename(cloud_path.replace(".tsv.gz", ".ndjson"));
	const TEMP_FILE_TRANSFORMED_PATH = path.join(TEMP_DIR, TEMP_FILE_TRANSFORMED);
	const writeStream = createWriteStream(TEMP_FILE_TRANSFORMED_PATH, { highWaterMark });
	writeStream.on('error', function (err) {
		log.error(err, "WRITE ERROR!");
	});

	//big 'ol parser
	Papa.parse(tsvStream, {
		header: true,
		fastMode: true,
		skipEmptyLines: true,
		transformHeader: (header, index) => headers[index]["Column name"],
		transform: cleanAdobeRaw,
		step: function (result) {
			const mpEvent = adobeToMixpanel(result.data);
			writeStream.write(JSON.stringify(mpEvent) + '\n');
		},
		complete: function (results, file) {
			tsvStream.destroy();
		}
	});

	//wait for stream to finish
	await new Promise((resolve, reject) => {
		tsvStream.on('end', () => {
			writeStream.end();
			resolve();
		}).on('error', err => {
			writeStream.end();
			reject(err);
		});
	});


	log.debug(f, 'uploading to cloud storage');
	const { file: upload_path } = u.parseGCSUri(dest_path);
	const destination = path.join(upload_path, TEMP_FILE_TRANSFORMED);
	const [uploaded] = await storage.bucket(bucket).upload(TEMP_FILE_TRANSFORMED_PATH, { destination, gzip: false });
	if (!isLocal) {
		await u.rm(TEMP_FILE_TRANSFORMED_PATH);
		await u.rm(TEMP_FILE_PATH);
	};
	timer.stop(false);
	log.debug(f, 'job done');
	return timer.report(false);
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
			"distinct_id":  row.mcvisid, //or what else?
			"time": Number(row.hit_time_gmt) || Number(row.cust_hit_time_gmt) || Number(row.last_hit_time_gmt), //Number(row.cust_hit_time_gmt),
			...u.removeNulls(row)
		}
	};

	const { distinct_id, time} = mixpanelEvent.properties;

	const hash = md5(`${distinct_id}-${time}`);
	mixpanelEvent.properties.$insert_id = hash;
	return mixpanelEvent;
}

// resolve row values to human readable values
function cleanAdobeRaw(value, header) {
	//set "" to null
	if (value === "") return null;
	//standard adobe dimensions
	if (enumerableLookups.includes(header?.toLowerCase())) {
		value = lookups[header.toLowerCase()].get(value);
		return value;
	}

	//post_event_list is where we define events; a "hit" is multiple events
	if (header === "post_event_list") {
		const events = value.split(',').map(a => a.trim());
		const eventNames = events.map(event => {
			//some events are like 704=20... where 704 is the custom event id and 20 is the duration
			if (event.includes("=")) {
				event = event.split("=")[0];
			}
			//resolving metrics
			if (metrics.get(event)) return metrics.get(event);

			//resolve standard events
			if (standardEventList.get(event)) return standardEventList.get(event);

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
GETTERS
----
*/


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

async function getHeaders(headersFile) {
	const rawFile = await u.load(headersFile);
	const parsedFile = Papa.parse(rawFile, { header: true }).data;
	return parsedFile;
}

export default main;