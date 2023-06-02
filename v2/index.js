const path = require('path');
const { createReadStream } = require('fs');
const Papa = require('papaparse');
const { zipObject } = require('lodash');
const md5 = require('md5');
const u = require('ak-tools');

const standardLookupsFolder = `./v2/lookups-standard/`;
const customEventListFile = `./v2/lookups-custom/eventList.csv`;
const headersFile = `./v2/lookups-custom/columns.csv`;

async function main(file) {
	const lookups = await getLookups(standardLookupsFolder);
	const enumerableLookups = Object.keys(lookups)
	const eventList = await getCustomLookups(customEventListFile);
	const headers = await getHeaders(headersFile);
	const rawFile = await u.load(file);
	const parsedRaw = Papa.parse(rawFile, {
		header: true,
		skipEmptyLines: true,
		transformHeader: (header, index) => {
			return headers[index]["Column name"]
		},
		transform: (value, header) => {
			if (enumerableLookups.includes(header?.toLowerCase())) {
				value = lookups[header.toLowerCase()].get(value);
				return value
			}
			
			// if (header === "post_event_list") {
			// 	const events = value.split(',').map(a => a.trim());
			// 	const eventNames = events.map(event => {
			// 		const lookup = eventList.find(lookup => lookup.event_id === event);
			// 		return lookup.event_name;
			// 	});
			// 	return eventNames
			// }
			return value
		}		
	}).data;

	debugger;
}



async function getLookups(standardLookupsFolder) {
	const standardLookups = await u.ls(path.resolve(standardLookupsFolder));
	const results = {};
	for (const lookup of standardLookups) {
		const lookupName = path.basename(lookup, '.csv').replace(".tsv", "");
		const rawFile = await u.load(lookup);
		const lookupData = Papa.parse(rawFile, { header: false}).data;
		const lookupMap = new Map(lookupData.map(i => [i[0], i[1]]));
		results[lookupName] = lookupMap;
	}
	return results;
}

async function getCustomLookups(customLookupsFile) {
	const rawFile = await u.load(customLookupsFile);
	const parsedFile = Papa.parse(rawFile, { header: true }).data;
	return parsedFile;
}

async function getHeaders(headersFile) {
	const rawFile = await u.load(headersFile);
	const parsedFile = Papa.parse(rawFile, { header: true }).data;
	return parsedFile;
}

module.exports = main;