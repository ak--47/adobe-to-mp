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

// Performance tuning constants
// For even better memory management, run Node.js with: node --expose-gc script.js
const PERFORMANCE = {
	GZIP_CHUNK_SIZE: 1024 * 1024,         // 1MB chunks for gzip decompression
	TRANSFORM_BUFFER_SIZE: 20000,         // Objects buffered in transform stream (2x increase)
	WRITE_BUFFER_SIZE: 16 * 1024 * 1024,  // 16MB buffer for file writes
	PARSE_CHUNK_SIZE: 1024 * 1024,        // 1MB chunks for CSV parsing
	PROGRESS_LOG_INTERVAL: 50000          // Log progress less frequently
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

// Pre-computed header transformation maps for performance
let evarMap = null;
let propMap = null;
let headerTransformMap = null;

// Lazy load lookup tables only when needed - with parallel loading
async function initializeLookups() {
	// Check if any lookups need to be loaded
	const needsLookups = !lookups;
	const needsHeaders = !headers;
	const needsEvents = !standardEventList;

	if (!needsLookups && !needsHeaders && !needsEvents) {
		return; // All already loaded
	}

	log.debug('Loading lookup tables in parallel...');
	const loadTimer = u.timer('lookup-loading');
	loadTimer.start();

	// Load all required lookups in parallel
	const promises = [];

	if (needsLookups) {
		promises.push(
			getLookups(`./lookups-standard/`).then(result => {
				lookups = result;
				enumerableLookups = Object.keys(lookups);
				return { type: 'lookups', count: enumerableLookups.length };
			})
		);
	}

	if (needsHeaders) {
		promises.push(
			getHeaders(`./lookups-custom/columns.csv`).then(result => {
				headers = result;
				return { type: 'headers', count: headers.length };
			})
		);
	}

	if (needsEvents) {
		promises.push(
			getHashMap(`./lookups-custom/events.tsv`).then(result => {
				standardEventList = result;
				return { type: 'events', count: standardEventList.size };
			})
		);
	}

	// Wait for all to complete
	log.debug(`Starting parallel loading of ${promises.length} lookup tables...`);
	const results = await Promise.all(promises);

	loadTimer.stop(false);
	log.debug(`Parallel loading completed in ${loadTimer.report(false).human}`);
	results.forEach(r => log.debug(`\t- Loaded ${r.count} ${r.type}`));
}

// Build pre-computed header transformation maps for ultra-fast lookups
function buildHeaderTransformMaps() {
	log.debug('Building header transformation maps...');
	
	// Convert customer lookups from arrays to Maps for O(1) lookups
	if (CUSTOMER_EVARS) {
		evarMap = new Map();
		CUSTOMER_EVARS.forEach(evar => {
			evarMap.set(evar["Evar #"], evar.Name);
		});
	}
	
	if (CUSTOMER_PROPS) {
		propMap = new Map();
		CUSTOMER_PROPS.forEach(prop => {
			propMap.set(prop["Property #"], prop.Name);
		});
	}
	
	// Pre-compute all header transformations
	headerTransformMap = new Map();
	headers.forEach((header, index) => {
		let transformedHeader = header.trim();
		
		// Pre-compute evar transformations
		if (evarMap && (header.toLowerCase().startsWith("evar") || header.toLowerCase().startsWith("post_evar"))) {
			const evarNum = header.match(/\d+/)?.[0];
			if (evarNum && evarMap.has(evarNum)) {
				transformedHeader = evarMap.get(evarNum);
			}
		}
		
		// Pre-compute prop transformations
		if (propMap && (header.toLowerCase().startsWith("prop") || header.toLowerCase().startsWith("post_prop"))) {
			const propNum = header.match(/\d+/)?.[0];
			if (propNum && propMap.has(propNum)) {
				transformedHeader = propMap.get(propNum);
			}
		}
		
		headerTransformMap.set(index, transformedHeader);
	});
	
	log.debug(`Pre-computed ${headerTransformMap.size} header transformations`);
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
	}
	else {
		CUSTOMER_EVARS = LOOKUPS.evars || null;
		CUSTOMER_PROPS = LOOKUPS.props || null;
		CUSTOMER_CUSTOM_EVENTS = LOOKUPS.custom_events || null;
		
		// Pre-compute header transformation maps for performance
		buildHeaderTransformMaps();
	}

	const timer = u.timer('transform');
	timer.start();


	let TEMP_FILE_TRANSFORMED, TEMP_FILE_TRANSFORMED_PATH, remoteFile;
	let downloadedFilePath = null; // Track downloaded GCS file for cleanup

	// Determine if we're in pure cloud mode (GCS input + GCS output)
	const isCloudMode = cloud_path.startsWith('gs://') && dest_path?.startsWith('gs://');
	// const isLocalMode = !cloud_path.startsWith('gs://'); // Unused but kept for clarity

	// Cloud storage setup
	if (cloud_path.startsWith('gs://') && !isCloudMode) {
		// Mixed mode: GCS input but local output - download file first
		try {
			log.debug(`Running in mixed mode, downloading to ${TEMP_DIR}`);
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

	// Setup file handling based on mode
	remoteFile = {};

	if (isCloudMode) {
		// Pure cloud mode - stream everything, no local files
		log.debug('Running in pure cloud mode - streaming input and output');
		const storage = new Storage();
		const { bucket: inputBucket, file: inputFile } = u.parseGCSUri(cloud_path);
		remoteFile.createReadStream = () => {
			return storage.bucket(inputBucket).file(inputFile).createReadStream();
		};

		// No local temp file in cloud mode
		TEMP_FILE_TRANSFORMED_PATH = null;

	} else {
		// Local mode or mixed mode - use local files
		log.debug(`Processing local file: ${cloud_path}`);
		remoteFile.createReadStream = () => {
			return fs.createReadStream(cloud_path);
		};

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

		if (fs.existsSync(TEMP_FILE_TRANSFORMED_PATH)) {
			fs.unlinkSync(TEMP_FILE_TRANSFORMED_PATH);
		}
	}


	// Setup output stream based on mode
	let writeStream;
	if (isCloudMode) {
		// Pure cloud mode - stream directly to GCS
		const storage = new Storage();
		const { bucket: outputBucket, file: outputPath } = u.parseGCSUri(dest_path);

		// Generate output filename
		let outputBaseName = path.basename(cloud_path);
		if (outputBaseName.endsWith('.tsv.gz')) {
			outputBaseName = outputBaseName.replace('.tsv.gz', '.ndjson');
		} else if (outputBaseName.endsWith('.tsv')) {
			outputBaseName = outputBaseName.replace('.tsv', '.ndjson');
		} else {
			const nameWithoutExt = path.parse(outputBaseName).name;
			outputBaseName = nameWithoutExt + '.ndjson';
		}

		const destination = path.join(outputPath, outputBaseName);
		log.debug(`Streaming output to: gs://${outputBucket}/${destination}`);

		writeStream = storage.bucket(outputBucket).file(destination).createWriteStream({
			metadata: {
				contentType: 'application/x-ndjson'
			}
		});

	} else {
		// Local/mixed mode - write to local file
		writeStream = createWriteStream(TEMP_FILE_TRANSFORMED_PATH, {
			highWaterMark: PERFORMANCE.WRITE_BUFFER_SIZE // Configurable write buffer
		});
	}

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
			// Ultra-fast O(1) lookup using pre-computed header transformations
			// This replaces expensive regex and array searches with a simple Map lookup
			return headerTransformMap ? headerTransformMap.get(index) : headers[index]?.trim();

			// LEAVE THIS IN
			if (!likelyHeader && NODE_ENV === "dev") debugger;

			if (CUSTOMER_EVARS) {
				if (likelyHeader?.toLowerCase()?.startsWith("evar") || likelyHeader?.toLowerCase()?.startsWith("post_evar")) {
					const evarNum = likelyHeader.match(/\d+/);
					if (!evarNum && NODE_ENV === "dev") debugger;
					const evar = CUSTOMER_EVARS.find(e => e["Evar #"] === evarNum[0]);
					if (evar) {
						likelyHeader = evar.Name;
					}
					// if (!evar && NODE_ENV === "dev") debugger
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

				//allow exploding events (optimized for performance)
				if (Array.isArray(mpEvent)) {
					// Remove debug logging from hot path for better performance
					// Batch process arrays more efficiently
					for (let i = 0; i < mpEvent.length; i++) {
						this.push(JSON.stringify(mpEvent[i]) + '\n');
					}
				} else {
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
	const pipelineTimer = u.timer('pipeline');
	pipelineTimer.start();

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
		pipelineTimer.stop(false);
		log.info(`... Pipeline completed successfully in ${pipelineTimer.report(false).human}`);
	} catch (err) {
		pipelineTimer.stop(false);
		log.error(err, `Pipeline error in ${pipelineTimer.report(false).human}`);
		// Clean up any partial files and downloaded files (only in local/mixed mode)
		if (!isCloudMode) {
			try {
				if (TEMP_FILE_TRANSFORMED_PATH && fs.existsSync(TEMP_FILE_TRANSFORMED_PATH)) {
					fs.unlinkSync(TEMP_FILE_TRANSFORMED_PATH);
				}
				if (downloadedFilePath && fs.existsSync(downloadedFilePath)) {
					fs.unlinkSync(downloadedFilePath);
				}
			} catch (cleanupErr) {
				log.warn(cleanupErr, 'Error during error cleanup');
			}
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

	if (isCloudMode) {
		// Pure cloud mode - output was streamed directly to GCS
		timer.stop(false);
		const { bucket: outputBucket, file: outputPath } = u.parseGCSUri(dest_path);
		let outputBaseName = path.basename(cloud_path);
		if (outputBaseName.endsWith('.tsv.gz')) {
			outputBaseName = outputBaseName.replace('.tsv.gz', '.ndjson');
		} else if (outputBaseName.endsWith('.tsv')) {
			outputBaseName = outputBaseName.replace('.tsv', '.ndjson');
		} else {
			const nameWithoutExt = path.parse(outputBaseName).name;
			outputBaseName = nameWithoutExt + '.ndjson';
		}
		const destination = path.join(outputPath, outputBaseName);
		return { ...timer.report(false), source: cloud_path, destination: `gs://${outputBucket}/${destination}` };

	} else if (dest_path?.startsWith('gs://')) {
		// Mixed mode - upload local file to GCS
		const storage = new Storage();
		const { bucket, file: upload_path } = u.parseGCSUri(dest_path);
		log.debug(`uploading to ${upload_path}`);

		// For GCS upload, keep the .ndjson extension (no compression)
		const uploadFileName = TEMP_FILE_TRANSFORMED; // Already has .ndjson extension
		const destination = path.join(upload_path, uploadFileName);
		const [uploaded] = await storage.bucket(bucket).upload(TEMP_FILE_TRANSFORMED_PATH, { destination, gzip: false });

		// Clean up temp files after successful upload
		await cleanupTempFiles();

		timer.stop(false);
		return { ...timer.report(false), source: cloud_path, destination: 'gs://'.concat(bucket).concat('/').concat(uploaded.name) };

	} else {
		// Local mode - keep the transformed output for local processing
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

async function getLookups(standardLookupsFolder) {
	const standardLookups = await u.ls(path.resolve(standardLookupsFolder));
	const results = {};
	for (const lookup of standardLookups) {
		const lookupName = path.basename(lookup, '.csv').replace(".tsv", "");

		// Load and parse file
		let rawFile = await u.load(lookup);
		let lookupData = Papa.parse(rawFile, { header: false }).data;

		// Create map and immediately null intermediate vars to free memory
		const lookupMap = new Map(lookupData.map(i => [i[0], i[1]]));
		rawFile = null; // Explicit cleanup
		lookupData = null; // Explicit cleanup

		results[lookupName] = lookupMap;

		// Force garbage collection hint (if available)
		if (global.gc && NODE_ENV === 'dev') {
			global.gc();
		}
	}
	return results;
}

async function getHashMap(customLookupsFile, replacePhrase, keyCol = 0, ValueCol = 1) {
	// Load and parse file
	let rawFile = await u.load(customLookupsFile);
	let parsedFile = Papa.parse(rawFile, { header: false }).data;

	// Create map and immediately null intermediate vars to free memory
	const lookup = new Map(parsedFile.map(i => {
		if (replacePhrase) return [i[keyCol].toString().replace(replacePhrase, "").toLowerCase(), i[ValueCol]];
		return [i[keyCol], i[ValueCol]];
	}));

	// Explicit cleanup
	rawFile = null;
	parsedFile = null;

	// Force garbage collection hint (if available)
	if (global.gc && NODE_ENV === 'dev') {
		global.gc();
	}

	return lookup;
}

async function getHeaders(headersFile) {
	// Load and parse file
	let rawFile = await u.load(headersFile);
	let parsedFile = Papa.parse(rawFile, { header: false }).data;

	// Get headers and immediately null intermediate vars to free memory
	const headers = parsedFile[0];

	// Explicit cleanup
	rawFile = null;
	parsedFile = null;

	// Force garbage collection hint (if available)
	if (global.gc && NODE_ENV === 'dev') {
		global.gc();
	}

	return headers;
}



export default main;