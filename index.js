const u = require('./utils.js');
const path = require('path');
const fs = require('fs').promises

async function main(rawFile, metaDatDir) {	
	const metaFiles = await u.listFiles((path.resolve(metaDatDir)));
	const headers = await u.getHeaders(metaFiles.column_headers);
	const rawParsed = await u.parseRaw(path.resolve(rawFile), headers);
	const metaLookup = await u.getLookups(metaFiles);

	debugger;
	return 42
}


module.exports = main;