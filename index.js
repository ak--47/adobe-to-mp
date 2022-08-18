const u = require('./utils.js');
const path = require('path');
const fs = require('fs').promises
const mpImport = require('mixpanel-import');

async function main(z_rawFile, z_metaDatDir, z_eVarDfns, mpCreds) {	
	// PARSE AND CLEAN
	const A_metaFiles = await u.listFiles((path.resolve(z_metaDatDir)));
	const B_headers = await u.getHeaders(A_metaFiles.column_headers);
	const C_rawParsed = await u.parseRaw(path.resolve(z_rawFile), B_headers);
	const D_dataSansEmpties = u.noNulls(C_rawParsed);

	// APPLY ADOBE LOOKUPS
	const E_metaLookup = await u.getLookups(A_metaFiles);
	const F_joinMetaAndRaw = await u.applyLookups(D_dataSansEmpties, E_metaLookup);
	const G_evarSource = await u.loadTSV(path.resolve(z_eVarDfns), true);
	const H_dataWithEvar = await u.applyEvars(F_joinMetaAndRaw, G_evarSource);	
	
	//TRANSFORM AND SEND TO MP
	const I_transformToMp = u.adobeToMp(H_dataWithEvar)
	
	const options = {
		recordType: `event`, //event, user, OR group
		streamSize: 27, // highWaterMark for streaming chunks (2^27 ~= 134MB)
		region: `US`, //US or EU
		recordsPerBatch: 2000, //max # of records in each batch
		bytesPerBatch: 2 * 1024 * 1024, //max # of bytes in each batch
		strict: true, //use strict mode?
		logs: true, //print to stdout?
	
		//a function reference to be called on every record
		//useful if you need to transform the data
		transformFunc: function noop(a) { return a }
	}
	// const importedData = await mpImport(creds, data, options);

	debugger;
	return 42
}


module.exports = main;