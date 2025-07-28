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
const { Transform, pipeline } = require('stream');
const { promisify } = require('util');
const pipelineAsync = promisify(pipeline);
const NODE_ENV = process.env.NODE_ENV || "unknown";
const TEMP_DIR = NODE_ENV === "dev" ? path.resolve("./tmp") : os.tmpdir();
const MB = 25;

// Performance tuning constants
const PERFORMANCE = {
	GZIP_CHUNK_SIZE: 64 * 1024 * 2,      // 128KB chunks for gzip decompression
	TRANSFORM_BUFFER_SIZE: 10000,     // Objects buffered in transform stream
	WRITE_BUFFER_SIZE: 1024 * 1024 * 10,  // 10MB buffer for file writes
	PARSE_CHUNK_SIZE: 64 * 1024,     // 64KB chunks for CSV parsing
	PROGRESS_LOG_INTERVAL: 10000     // Log progress every N rows
};

import { log } from './logger.js';

/*
----
LAZY-LOADED LOOKUP TABLES
----
*/

// Cache for lookup tables to prevent re-loading
let lookups = null;
let enumerableLookups = null;
let headers = null;
let standardEventList = null;

// optional customer supplied lookups
let CUSTOMER_EVARS = null;
let CUSTOMER_PROPS = null;
let CUSTOMER_CUSTOM_EVENTS = null;

// Lazy load lookup tables only when needed
async function initializeLookups() {
	if (!lookups) {
		log.info('Loading standard Adobe lookups...');
		lookups = await getLookups(`./lookups-standard/`);
		enumerableLookups = Object.keys(lookups);
		log.info(`Loaded ${enumerableLookups.length} standard lookups`);
	}
	
	if (!headers) {
		log.info('Loading column headers...');
		headers = await getHeaders(`./lookups-custom/columns.csv`);
		log.info(`Loaded ${headers.length} column headers`);
	}
	
	if (!standardEventList) {
		log.info('Loading standard event list...');
		standardEventList = await getHashMap(`./lookups-custom/events.tsv`);
		log.info(`Loaded ${standardEventList.size} standard events`);
	}
}



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
	if (!cloud_path) {
		throw new Error("cloud_path is required");
	}
	
	// Initialize lookup tables on first use
	await initializeLookups();
	
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

	// Memory monitoring in development
	// if (NODE_ENV === 'dev') {
	// 	const initialMemory = process.memoryUsage();
	// 	log.debug({ memory: initialMemory }, 'Initial memory usage');
		
	// 	// Monitor memory every 10 seconds during processing
	// 	const memoryMonitor = setInterval(() => {
	// 		const currentMemory = process.memoryUsage();
	// 		log.debug({ 
	// 			rss: Math.round(currentMemory.rss / 1024 / 1024) + 'MB',
	// 			heapUsed: Math.round(currentMemory.heapUsed / 1024 / 1024) + 'MB',
	// 			heapTotal: Math.round(currentMemory.heapTotal / 1024 / 1024) + 'MB'
	// 		}, 'Memory usage');
	// 	}, 10000);
		
	// 	// Clean up monitor after processing
	// 	process.on('exit', () => clearInterval(memoryMonitor));
	// 	process.on('SIGINT', () => clearInterval(memoryMonitor));
	// }

	let TEMP_FILE_TRANSFORMED, TEMP_FILE_TRANSFORMED_PATH, remoteFile;
	let downloadedFilePath = null; // Track downloaded GCS file for cleanup

	//cloud storage setup - download file to temp first
	if (cloud_path.startsWith('gs://')) {
		try {
			log.debug(`Running in cloud mode, downloading to ${TEMP_DIR}`);
			const storage = new Storage();
			const { bucket, file: cloudURI } = u.parseGCSUri(cloud_path);
			const filename = path.basename(cloud_path);
			const localFilePath = path.join(TEMP_DIR, filename);
			
			// Clean up any existing downloaded file
			if (fs.existsSync(localFilePath)) {
				log.debug(`Removing existing file: ${localFilePath}`);
				fs.unlinkSync(localFilePath);
			}
			
			log.info(`Downloading ${cloud_path} to ${localFilePath}`);
			const downloadTimer = u.timer('download');
			downloadTimer.start();
			
			// Use the simple download method (should be fixed in v7.16.0)
			const file = storage.bucket(bucket).file(cloudURI);
			await file.download({ destination: localFilePath });
			log.debug('Download completed successfully');
			
			downloadTimer.stop(false);
			log.info(`Download completed in ${downloadTimer.report(false).human}`);
			
			// Now treat it as a local file
			downloadedFilePath = localFilePath; // Remember for cleanup
			cloud_path = localFilePath;
			FILE_IS_GZIPPED = localFilePath.endsWith('.gz');
			
		}
		catch (err) {
			log.error(err, "Error downloading cloud file");
			throw err;
		}
	}

	//local file setup (now handles both original local files and downloaded GCS files)
	remoteFile = {};
	
	// Generate correct output filename (always .ndjson, never .gz since we're creating uncompressed output)
	let baseName = path.basename(cloud_path);
	if (baseName.endsWith('.tsv.gz')) {
		baseName = baseName.replace('.tsv.gz', '.ndjson');
	} else if (baseName.endsWith('.tsv')) {
		baseName = baseName.replace('.tsv', '.ndjson');
	} else {
		// Fallback for other file extensions
		const nameWithoutExt = path.parse(baseName).name;
		baseName = nameWithoutExt + '.ndjson';
	}
	
	TEMP_FILE_TRANSFORMED = baseName;
	TEMP_FILE_TRANSFORMED_PATH = path.join(TEMP_DIR, TEMP_FILE_TRANSFORMED);
	log.debug(`Processing local file: ${cloud_path} -> ${TEMP_FILE_TRANSFORMED_PATH}`);
	remoteFile.createReadStream = () => {
		return fs.createReadStream(cloud_path);
	};
	if (fs.existsSync(TEMP_FILE_TRANSFORMED_PATH)) {
		fs.unlinkSync(TEMP_FILE_TRANSFORMED_PATH);
	}


	const writeStream = createWriteStream(TEMP_FILE_TRANSFORMED_PATH, {
		highWaterMark: PERFORMANCE.WRITE_BUFFER_SIZE // Configurable write buffer
	});
	writeStream.on('error', function (err) {
		log.warn(err, "WRITE ERROR - attempting to continue");
		// Don't immediately fail on write errors
	});

	const parseStream = Papa.parse(Papa.NODE_STREAM_INPUT, {
		header: true,
		fastMode: true,
		skipEmptyLines: true,
		chunkSize: PERFORMANCE.PARSE_CHUNK_SIZE, // Configurable parsing chunk size
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
		log.warn(err, "PARSE ERROR - continuing processing");
		// Don't stop processing on parse errors
	});

	let processedRows = 0;
	const transformStream = new Transform({
		objectMode: true,
		highWaterMark: PERFORMANCE.TRANSFORM_BUFFER_SIZE, // Configurable transform buffer
		transform(chunk, encoding, callback) {
			try {
				processedRows++;

				// Log progress less frequently to reduce overhead
				if (NODE_ENV === 'dev' && (processedRows <= 3 || processedRows % PERFORMANCE.PROGRESS_LOG_INTERVAL === 0)) {
					log.debug(`Processed ${processedRows} rows`);
				}

				// Skip null/empty chunks (remove debug logging for performance)
				if (!chunk) {
					callback();
					return;
				}

				const mpEvent = adobeToMixpanel(chunk);

				// Skip null results (remove debug logging for performance)
				if (!mpEvent) {
					callback();
					return;
				}

				//allow exploding events
				if (Array.isArray(mpEvent)) {
					if (NODE_ENV === 'dev' && processedRows <= 3) {
						log.debug(`Generated ${mpEvent.length} events from row ${processedRows}`);
					}
					for (const event of mpEvent) {
						this.push(JSON.stringify(event) + '\n');
					}
				} else {
					if (NODE_ENV === 'dev' && processedRows <= 3) {
						log.debug(`Generated 1 event from row ${processedRows}`);
					}
					this.push(JSON.stringify(mpEvent) + '\n');
				}
				
				callback();
			} catch (err) {
				log.error(err, 'Transform error');
				callback(err);
			}
		}
	});

	transformStream.on('error', function (err) {
		if (NODE_ENV === "dev") debugger;
		log.warn(err, "TRANSFORM ERROR - continuing processing");
		// Don't stop processing on transform errors
	});





	// Simple local file streaming (much more reliable)
	log.info('Starting pipeline...');
	
	// Build pipeline components with optimized settings
	const pipelineComponents = [remoteFile.createReadStream()];
	
	// Add gzip decompression if needed
	if (FILE_IS_GZIPPED) {
		const gunzipStream = zlib.createGunzip({
			chunkSize: PERFORMANCE.GZIP_CHUNK_SIZE // Configurable gzip chunk size
		});
		pipelineComponents.push(gunzipStream);
	}
	
	// Add processing stages
	pipelineComponents.push(parseStream, transformStream, writeStream);

	// Use Node.js pipeline - now with local files this should be rock solid
	try {
		await pipelineAsync(...pipelineComponents);
		log.info('Pipeline completed successfully');
	} catch (err) {
		log.error(err, 'Pipeline error');
		// Clean up any partial files and downloaded files
		try {
			if (fs.existsSync(TEMP_FILE_TRANSFORMED_PATH)) {
				fs.unlinkSync(TEMP_FILE_TRANSFORMED_PATH);
			}
			if (downloadedFilePath && fs.existsSync(downloadedFilePath)) {
				fs.unlinkSync(downloadedFilePath);
			}
		} catch (cleanupErr) {
			log.warn(cleanupErr, 'Error during error cleanup');
		}
		throw err;
	}


	// Clean up function to remove temp files
	const cleanupTempFiles = async () => {
		try {
			// Clean up downloaded GCS file
			if (downloadedFilePath && fs.existsSync(downloadedFilePath)) {
				log.debug(`Cleaning up downloaded file: ${downloadedFilePath}`);
				fs.unlinkSync(downloadedFilePath);
			}
			
			// Clean up transformed output file (only in dev or after upload)
			if (NODE_ENV === 'dev' || dest_path?.startsWith('gs://')) {
				if (fs.existsSync(TEMP_FILE_TRANSFORMED_PATH)) {
					log.debug(`Cleaning up transformed file: ${TEMP_FILE_TRANSFORMED_PATH}`);
					fs.unlinkSync(TEMP_FILE_TRANSFORMED_PATH);
				}
			}
		} catch (cleanupErr) {
			log.warn(cleanupErr, 'Error during temp file cleanup');
		}
	};

	if (dest_path?.startsWith('gs://')) {
		const storage = new Storage();
		const { bucket, file: upload_path } = u.parseGCSUri(dest_path);
		log.debug(`uploading to ${upload_path}`);
		
		// For GCS upload, add .gz extension since we're compressing during upload
		const uploadFileName = TEMP_FILE_TRANSFORMED.replace('.ndjson', '.ndjson.gz');
		const destination = path.join(upload_path, uploadFileName);
		const [uploaded] = await storage.bucket(bucket).upload(TEMP_FILE_TRANSFORMED_PATH, { destination, gzip: true });
		
		// Clean up temp files after successful upload
		await cleanupTempFiles();
		
		timer.stop(false);
		return { ...timer.report(false), source: cloud_path, destination: 'gs://'.concat(bucket).concat('/').concat(uploaded.name) };

	}
	else {
		// Clean up downloaded file but keep the transformed output for local processing
		if (downloadedFilePath && fs.existsSync(downloadedFilePath)) {
			log.debug(`Cleaning up downloaded file: ${downloadedFilePath}`);
			fs.unlinkSync(downloadedFilePath);
		}
		
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

	// Early return for empty/invalid rows to save memory
	if (!row || Object.keys(row).length === 0) {
		return null;
	}

	const time =
		Number(row.hit_time_gmt) ||
		Number(row.cust_hit_time_gmt) ||
		Number(row.last_hit_time_gmt);

	// 1. PREPARE: Gather base properties and identifiers
	const baseProperties = {
		time,
		distinct_id: row.mcvisid,
		...row
	};

	// Use the more reliable visitor ID if available
	if ((row.visid_high !== "0" && row.visid_high) || (row.visid_low !== "0" && row.visid_low)) {
		baseProperties.distinct_id = `${row.visid_high}${row.visid_low}`;
	}

	const hitHash = quickHash(`${row?.hitid_high || ""}-${row?.hitid_low || ""}`);
	const isPageView = row.post_page_event === "0" || row.post_page_event === 0;

	// A more robust set of events that are really measurements/properties
	const MEASUREMENT_EVENTS = new Set([
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
		'Searchlight Content Health Score',
		// 'Page Scroll 25',
		// 'Page Scroll 50',
		// 'Page Scroll 75',
		// 'Page Scroll 100',
		// 'accordionExpanded',
		// 'accordionCollapse'		
	]);

	// Events that are usually noise or internal tracking details from Adobe
	const IGNORED_EVENTS = new Set([
		'Page Name', // This is a dimension, not an event
		'Instance of eVar4',
		'Instance of eVar11',
		'Instance of eVar32',
		'Filter' // Internal Adobe filtering events
	]);

	const allEventItems = row.post_event_list || [];
	const measurementProperties = {};
	const realEventNames = [];

	// Separate event_list into real events vs. measurements vs. ignored
	for (const eventItem of allEventItems) {
		const eventName = typeof eventItem === 'string' ? eventItem : eventItem.name;
		const eventValue = typeof eventItem === 'object' ? eventItem.value : null;

		if (IGNORED_EVENTS.has(eventName)) continue;

		if (MEASUREMENT_EVENTS.has(eventName)) {
			const propKey = eventName.toLowerCase().replace(/\s+/g, '_');
			// Use the numeric value if available, otherwise mark as true
			measurementProperties[propKey] = eventValue || row[eventName] || true;
		} else {
			realEventNames.push({ name: eventName, value: eventValue });
		}
	}

	// 2. DETERMINE PRIMARY & SECONDARY EVENTS
	const finalEvents = [];

	if (isPageView) {
		// The primary event for this hit is "Page Viewed"
		const pageViewEvent = {
			event: "Page Viewed",
			...baseProperties,
			...measurementProperties, // Add measurements to page view
			page_name: row["Page Name"] || row.post_pagename || row.pagename
		};
		finalEvents.push(pageViewEvent);

		// Create additional events for other real business actions on that page load
		for (const eventItem of realEventNames) {
			finalEvents.push({
				event: eventItem.name,
				...baseProperties,
				...measurementProperties, // Add measurements to all events
				...(eventItem.value && { event_value: eventItem.value })
			});
		}

	} else {
		// This is a Link Tracking hit (s.tl). Do NOT create "Page Viewed".
		// Find the most important action to be the event name.
		if (realEventNames.length > 0) {
			for (const eventItem of realEventNames) {
				finalEvents.push({
					event: eventItem.name,
					...baseProperties,
					...measurementProperties, // Add measurements to all events
					...(eventItem.value && { event_value: eventItem.value })
				});
			}
		} else if (Object.keys(measurementProperties).length > 0) {
			// Fallback if no "real" events are found, but we have measurements
			const fallbackEvent = {
				event: "Action Tracked",
				...baseProperties,
				...measurementProperties
			};
			finalEvents.push(fallbackEvent);
		}
	}

	// 3. GENERATE FINAL OUTPUT with unique insert_ids
	if (finalEvents.length === 0) {
		// Absolute fallback: if no events could be determined at all
		return {
			event: isPageView ? "Page Viewed" : "Unknown Action",
			...baseProperties,
			insert_id: hitHash // Only one event, no index needed
		};
	}

	// Assign unique insert_id and nudge timestamps for proper sequencing
	const processedEvents = finalEvents.map((evt, index) => {
		evt.insert_id = `${hitHash}-${index}`;
		// Nudge timestamps by 5 seconds per index to create logical sequence
		// First event (index 0) keeps original time, subsequent events get +5s each
		evt.time = evt.time + (index * 5);
		return evt;
	});

	// Always return array to ensure consistent timestamp nudging
	// The stream processor will handle flattening for output
	return processedEvents;
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
				// if (NODE_ENV === "dev") debugger;
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