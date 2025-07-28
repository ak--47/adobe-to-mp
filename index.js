/*
----
DEPENDENCIES
----
*/
import { createRequire } from 'module';
const require = createRequire(import.meta.url);

require('dotenv').config();
const Papa = require('papaparse');

const u = require('ak-tools');
const path = require('path');
const os = require('os');
const { Storage } = require('@google-cloud/storage');

const { createWriteStream } = require('fs');
const zlib = require('zlib');
const fs = require('fs');
const { Transform } = require('stream');
const NODE_ENV = process.env.NODE_ENV || "unknown";
const TEMP_DIR = NODE_ENV === "dev" ? path.resolve("./tmp") : os.tmpdir();
const MB = 25;

const bunyan = require('bunyan');
const { LoggingBunyan } = require('@google-cloud/logging-bunyan');
const bunyanFormat = require('bunyan-format');
const loggingBunyanStream = new LoggingBunyan({ logName: 'adobe-transform', redirectToStdout: false });

const loggerStreams = [];
// 1. Add the pretty-printing stream for local development
if (NODE_ENV === 'dev') {
	loggerStreams.push({
		stream: bunyanFormat({ outputMode: 'short', color: true }), // Pretty print with colors
		level: 'debug' // Show all debug messages in dev
	});
}

loggerStreams.push(
	loggingBunyanStream.stream('info') // Send info and above to Cloud Logging
);


export const log = bunyan.createLogger({
	name: 'adobe-transform',
	streams: loggerStreams
});


/*
----
CUSTOMER SUPPLIED LOOKUP TABLES
----
*/

// standard adobe prop values
const lookups = await getLookups(`./lookups-standard/`);
const enumerableLookups = Object.keys(lookups);

// required customer supplied lookups
const headers = await getHeaders(`./lookups-custom/columns.csv`);
const standardEventList = await getHashMap(`./lookups-custom/events.tsv`);

// optional customer supplied lookups
let CUSTOMER_EVARS = null;
let CUSTOMER_PROPS = null;
let CUSTOMER_CUSTOM_EVENTS = null;



/*
----
MAIN
----
*/


/** @typedef {Object} CustomerLookups
 * @property {Array<Object>} evars
 * @property {Array<Object>} props
 * @property {Array<Object>} custom_events
 */

/**
 * @param  {string} cloud_path
 * @param  {string} [dest_path] 
 * @param  {CustomerLookups} LOOKUPS={}
 */
async function main(cloud_path, dest_path, LOOKUPS = {}) {
	let FILE_IS_GZIPPED = false;
	if (cloud_path.endsWith('.gz')) {
		FILE_IS_GZIPPED = true;
	}

	if (Object.keys(LOOKUPS).length === 0) {
		log.info('no customer lookups provided, no evars, props, or customer events will be resolved');
	} else {
		CUSTOMER_EVARS = LOOKUPS.evars || null;
		CUSTOMER_PROPS = LOOKUPS.props || null;
		CUSTOMER_CUSTOM_EVENTS = LOOKUPS.custom_events || null;
	}

	const timer = u.timer('transform');
	timer.start();

	let TEMP_FILE_TRANSFORMED, TEMP_FILE_TRANSFORMED_PATH, remoteFile;

	//cloud storage setup
	if (cloud_path.startsWith('gs://')) {
		try {
			log.debug(`Running in cloud mode, using ${TEMP_DIR}`);
			const storage = new Storage();
			const { bucket, file: cloudURI } = u.parseGCSUri(cloud_path);
			const filename = path.basename(cloud_path);
			const f = { file: filename };
			TEMP_FILE_TRANSFORMED = path.basename(cloud_path.replace(".tsv.gz", ".ndjson"));
			TEMP_FILE_TRANSFORMED_PATH = path.join(TEMP_DIR, TEMP_FILE_TRANSFORMED);
			log.debug(f, 'streaming + transforming');
			remoteFile = storage.bucket(bucket).file(cloudURI);
		}
		catch (err) {
			log.error(err, "Error parsing cloud path");
		}
	}

	//local file setup
	if (!cloud_path.startsWith('gs://')) {
		remoteFile = {};
		TEMP_FILE_TRANSFORMED = path.basename(cloud_path.replace(".tsv.gz", ".ndjson"));
		TEMP_FILE_TRANSFORMED = path.basename(cloud_path.replace(".tsv", ".ndjson"));
		TEMP_FILE_TRANSFORMED_PATH = path.join(TEMP_DIR, TEMP_FILE_TRANSFORMED);
		log.debug(`Running in local mode, using ${TEMP_FILE_TRANSFORMED_PATH} as temp file`);
		remoteFile.createReadStream = () => {
			return fs.createReadStream(cloud_path);
		};

	}
	if (fs.existsSync(TEMP_FILE_TRANSFORMED_PATH)) {
		fs.unlinkSync(TEMP_FILE_TRANSFORMED_PATH);
	}


	const writeStream = createWriteStream(TEMP_FILE_TRANSFORMED_PATH);
	writeStream.on('error', function (err) {
		log.error(err, "WRITE ERROR!");
	});

	const parseStream = Papa.parse(Papa.NODE_STREAM_INPUT, {
		header: true,
		fastMode: true,
		skipEmptyLines: true,
		transformHeader: function (header, index) {
			// debugger;
			let likelyHeader;
			likelyHeader = headers[index].trim();
			if (!likelyHeader && NODE_ENV === "dev") debugger;
			if (CUSTOMER_EVARS) {
				if (likelyHeader?.toLowerCase()?.startsWith("evar") || likelyHeader?.toLowerCase()?.startsWith("post_evar")) {
					const evarNum = likelyHeader.match(/\d+/);
					if (!evarNum && NODE_ENV === "dev") debugger;
					const evar = CUSTOMER_EVARS.find(e => e["Evar #"] === evarNum[0]);
					if (evar) {
						likelyHeader = evar.Name;
					}
					// if (!evar && NODE_ENV === "dev") debugger;



				}
			}
			if (CUSTOMER_PROPS) {
				if (likelyHeader?.toLowerCase()?.startsWith("prop") || likelyHeader?.toLowerCase()?.startsWith("post_prop")) {
					const propNum = likelyHeader.match(/\d+/);
					if (!propNum && NODE_ENV === "dev") debugger;
					const prop = CUSTOMER_PROPS.find(p => p["Property #"] === propNum[0]);
					if (prop) {
						likelyHeader = prop.Name;
					}
					// if (!prop && NODE_ENV === "dev") debugger;
				}
			}
			// if (CUSTOMER_CUSTOM_EVENTS) {
			// 	if (likelyHeader?.toLowerCase()?.includes("event")) {
			// 		const customEventNum = likelyHeader.match(/\d+/);
			// 		if (!customEventNum && NODE_ENV === "dev") debugger;
			// 		const customEvent = CUSTOMER_CUSTOM_EVENTS.find(ce => ce["Custom Event #"] === customEventNum[0]);
			// 		if (customEvent) {
			// 			likelyHeader = customEvent.Name;
			// 		}
			// 		if (!customEvent && NODE_ENV === "dev") debugger;
			// 	}

			// }

			return likelyHeader;

		},
		transform: cleanAdobeRaw,
		newline: '\n',
		delimiter: '\t'
	});

	parseStream.on('error', function (err) {
		if (NODE_ENV === "dev") debugger;
		log.error(err, "PARSE ERROR!");
	});

	const transformStream = new Transform({
		objectMode: true, // this allows passing objects
		transform(chunk, encoding, callback) {
			const mpEvent = adobeToMixpanel(chunk);

			//allow exploding events
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
		if (NODE_ENV === "dev") debugger;
		log.error(err, "TRANSFORM ERROR!");
	});





	//pipeline
	await new Promise((resolve, reject) => {
		let stream = remoteFile.createReadStream();
		if (FILE_IS_GZIPPED) stream = stream.pipe(zlib.createGunzip());

		stream
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


	if (dest_path?.startsWith('gs://')) {
		const { file: upload_path } = u.parseGCSUri(dest_path);
		log.debug(`uploading to ${upload_path}`);
		const destination = path.join(upload_path, TEMP_FILE_TRANSFORMED);
		const [uploaded] = await storage.bucket(bucket).upload(TEMP_FILE_TRANSFORMED_PATH, { destination, gzip: true });
		if (NODE_ENV === 'dev') {
			await u.rm(TEMP_FILE_TRANSFORMED_PATH);
		}
		timer.stop(false);
		return { ...timer.report(false), source: cloud_path, destination: 'gs://'.concat(bucket).concat('/').concat(uploaded.name) };

	}
	else {
		timer.stop(false);
		return { ...timer.report(false), source: cloud_path, destination: TEMP_FILE_TRANSFORMED_PATH };
	}

}

/*
----
TRANSFORMS
----
*/

//transform adobe to mixpanel
function adobeToMixpanel(row) {
	u.removeNulls(row);

	const time =
		Number(row.hit_time_gmt) ||
		Number(row.cust_hit_time_gmt) ||
		Number(row.last_hit_time_gmt);

	// Base properties shared across all events from this hit
	const baseProperties = {
		time,
		distinct_id: row.mcvisid,
		...row
	};

	//use visid_high and visid_low if it's available
	if ((row.visid_high !== "0" && row.visid_high) || (row.visid_low !== "0" && row.visid_low)) {
		baseProperties.distinct_id = `${row.visid_high}${row.visid_low}`;
	}

	// Generate insert_id for deduplication
	const hash = quickHash(`${row?.hitid_high || ""}-${row?.hitid_low || ""}`);
	baseProperties.insert_id = hash;

	const events = [];
	let eventIndex = 0;

	// Check hit type based on post_page_event
	const isPageView = row.post_page_event === "0" || row.post_page_event === 0;

	if (isPageView) {
		// This is a Page View hit (s.t() call)
		// Create a "Page Viewed" event
		const pageViewEvent = {
			event: "Page Viewed",
			...baseProperties,
			insert_id: `${hash}-${eventIndex}`,
			page_name: row["Page Name"] || row.post_pagename || row.pagename // Use resolved page name
		};
		events.push(pageViewEvent);
		eventIndex++;
	}

	// Process additional events from post_event_list (both page views and link tracking hits)
	if (row.post_event_list && Array.isArray(row.post_event_list) && row.post_event_list.length > 0) {
		// Events that should be properties, not separate events (typically metrics/measurements)
		const propertyEvents = new Set([
			'Page Load Time',
			'Page Load Time Previous Page',
			'Time Spent on Page',
			'Download Time',
			'Connection Speed',
			'Bandwidth',
			'Screen Resolution',
			'Color Depth',
			'Java Version',
			'Flash Version',
			'Monitor Resolution',
			'Browser Height',
			'Browser Width',
			'File Size',
			'Form Field Progress',
			'Instance of eVar11', // This seems like a tracking instance, not an event
			'Instance of eVar32', // Similar tracking instance
			'Filter', // This seems like a technical/system event
			'Searchlight Content Health Score', // This is a metric
			'accordionExpanded', // UI state changes
			'accordionCollapse',
			'Ceros Component Click Event' // Technical tracking events
		]);

		// Separate events into real events vs properties
		const realEvents = [];
		const eventProperties = {};

		row.post_event_list.forEach(eventItem => {
			const eventName = typeof eventItem === 'string' ? eventItem : eventItem.name;
			const eventValue = typeof eventItem === 'object' ? eventItem.value : null;

			// Skip "Page Name" events since those are handled above for page views
			if (eventName === "Page Name") {
				return;
			}

			// If this is a measurement/metric, add it as a property
			if (propertyEvents.has(eventName)) {
				const propertyKey = eventName.toLowerCase().replace(/\s+/g, '_');
				eventProperties[propertyKey] = eventValue || true;
			} else {
				// This is a real business event
				realEvents.push(eventItem);
			}
		});

		// Add measurement properties to the main event if we have any
		if (Object.keys(eventProperties).length > 0 && events.length > 0) {
			events[0] = { ...events[0], ...eventProperties };
		}

		// If we have properties but no main event yet, create one for non-page view hits
		if (Object.keys(eventProperties).length > 0 && events.length === 0) {
			events.push({
				event: "Action Tracked",
				...baseProperties,
				...eventProperties,
				insert_id: `${hash}-${eventIndex}`
			});
			eventIndex++;
		}

		// Create separate events for real business events
		const eventObjects = realEvents.map((eventItem) => {
			const eventName = typeof eventItem === 'string' ? eventItem : eventItem.name;
			const eventValue = typeof eventItem === 'object' ? eventItem.value : null;

			const mixpanelEvent = {
				event: eventName,
				...baseProperties,
				insert_id: `${hash}-${eventIndex}`
			};

			// Add event value if present
			if (eventValue) {
				mixpanelEvent.event_value = eventValue;
			}

			eventIndex++;
			return mixpanelEvent;
		});

		events.push(...eventObjects);
	}

	// Return single event or array based on count
	if (events.length === 1) {
		return events[0];
	} else if (events.length > 1) {
		return events;
	}

	// Fallback: No meaningful events found
	return {
		event: isPageView ? "Page Viewed" : "Unknown Action",
		...baseProperties,
		insert_id: hash
	};
}

// resolve row values to human readable values
function cleanAdobeRaw(value, header, foo) {
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

	//deal with event_list, which is basically nested properties
	if (header?.toLowerCase()?.includes("event_list")) {
		const eventList = value.split(',').map(a => a.trim());
		//first lookup in events.tsv
		const events = eventList.map(eventItem => {
			let evNum = eventItem;
			let evValue = null;

			// Handle event=value format (e.g., "704=20")
			if (eventItem.includes("=")) {
				[evNum, evValue] = eventItem.split("=");
			}

			const genericName = standardEventList.get(evNum);
			if (genericName) {
				if (CUSTOMER_CUSTOM_EVENTS) {
					const customEventNum = genericName.match(/\d+/);
					if (customEventNum) {
						const customEvent = CUSTOMER_CUSTOM_EVENTS.find(ce => ce["Event"] === `event${customEventNum[0]}`);
						if (customEvent) {
							if (evValue) {
								return {
									name: customEvent.Name,
									value: evValue
								};
							}
							else {
								return customEvent.Name;
							}
						}
					}
				}
				// Fallback to generic name if no custom event found
				if (evValue) {
					return {
						name: genericName,
						value: evValue
					};
				}
				return genericName;
			}
			//if we can't resolve the event name, return the original format
			else {
				if (NODE_ENV === "dev") debugger;
				return eventItem; // Return original format (e.g., "999" or "999=25")
			}
		});

		value = events;
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
	// if (header === "post_event_list") {
	// 	const events = value.split(',').map(a => a.trim());
	// 	const eventNames = events.map(event => {
	// 		//some events are like 704=20... where 704 is the custom event id and 20 is the duration
	// 		if (event.includes("=")) {
	// 			event = event.split("=")[0];
	// 		}

	// 		//resolving metrics from metrics.csv
	// 		// if (metrics.get(event)) return metrics.get(event);

	// 		//resolve standard events from eventStandard.csv, although this should almost never happen
	// 		else if (standardEventList.get(event)) return standardEventList.get(event);

	// 		//if we can't resolve the event name, return it's number
	// 		else {
	// 			return event;
	// 		}
	// 	});

	// 	return eventNames.filter(a => a);
	// }
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
	const parsedFile = Papa.parse(rawFile, { header: false }).data;
	return parsedFile[0];
}

function* chunks(arr, n) {
	for (let i = 0; i < arr.length; i += n) {
		yield arr.slice(i, i + n);
	}
}

/**
 * Generates a non-cryptographic hash from a string using the DJB2 algorithm,
 * and returns it as a hexadecimal string.
 * It's fast and "good enough" for many uniqueness checks (e.g., internal IDs,
 * basic caching keys) where cryptographic security or perfect collision
 * resistance isn't required.
 *
 * @param {string} str The input string to hash.
 * @returns {string} The generated hash as an 8-character hexadecimal string.
 */
function quickHash(str) {
	let hash = 5381; // Initial hash value (prime number)
	let i = str.length;

	while (i) {
		// Multiply by 33 and XOR with the character code
		hash = (hash * 33) ^ str.charCodeAt(--i);
	}

	// Convert to an unsigned 32-bit integer, then to a hexadecimal string,
	// and pad with leading zeros to ensure a consistent 8-character length.
	return (hash >>> 0).toString(16).padStart(8, '0');
}

export default main;