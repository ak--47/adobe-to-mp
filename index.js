const u = require('./utils.js');
const path = require('path');
const fs = require('fs').promises
const mpImport = require('mixpanel-import');

async function main(z_rawFile, z_metaDatDir, z_Guides = {
    evars: `./`,
    custEvents: `./`,
    custProps: `./`
}, mpCreds) {
    // PARSE AND CLEAN
	u.time('parse raw')
    const A_metaFiles = await u.listFiles((path.resolve(z_metaDatDir)));
    const B_headers = await u.getHeaders(A_metaFiles.column_headers);
    const C_rawParsed = await u.parseRaw(path.resolve(z_rawFile), B_headers);
	u.time('parse raw', 'stop')
    

    // PARSE ADOBE LOOKUPS
	u.time('parse lookups')
    const z_metaLookup = await u.getLookups(A_metaFiles);
	const z_evars = (await u.loadTSV(path.resolve(z_Guides.evars), true)).map((infos)=> [infos.id.split('/')[1], infos.name])
	const z_custEvents = (await u.loadTSV(path.resolve(z_Guides.custEvents), true)).map((infos)=> [infos.id.split('/')[1], infos.name])
	const z_custProps = (await u.loadTSV(path.resolve(z_Guides.custProps), true)).map((infos)=> [infos.id.split('/')[1], infos.name])
	const E_enrichedLookups = u.enrichEventList(z_metaLookup, [...z_evars, ...z_custEvents, ...z_custProps]);
	const F_allLookups = {
		...E_enrichedLookups,
		evars : z_evars,
		custEvent: z_custEvents,
		custProps : z_custProps
	}
	u.time('parse lookups', 'stop')
    

	// APPLY ADOBE LOOKUPS
	u.time('apply lookups to raw')
	const G_joinMetaAndRaw = u.applyLookups(C_rawParsed, F_allLookups);

	// get rid of empty values
	const H_dataSansEmpties = u.noNulls(G_joinMetaAndRaw);
	u.time('apply lookups to raw', 'stop')
    //TRANSFORM AND SEND TO MP

	u.time('transform')
    const I_transformToMp = u.adobeToMp(H_dataSansEmpties)
	u.time('transform', 'stop')

    const options = {
        recordType: `event`, //event, user, OR group
        streamSize: 27, // highWaterMark for streaming chunks (2^27 ~= 134MB)
        region: `US`, //US or EU
        recordsPerBatch: 2000, //max # of records in each batch
        bytesPerBatch: 2 * 1024 * 1024, //max # of bytes in each batch
        strict: false, //use strict mode?
        logs: false, //print to stdout?

        //a function reference to be called on every record
        //useful if you need to transform the data
        transformFunc: function noop(a) { return a }
    }
	u.time('flush')
    const importedData = await mpImport(mpCreds, I_transformToMp, options);
	u.time('flush', 'stop')

    return importedData
}


module.exports = main;