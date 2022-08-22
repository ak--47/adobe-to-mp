const u = require('./utils.js');
const path = require('path');
const fs = require('fs').promises
const mpImport = require('mixpanel-import');
const os = require('os')


async function main(folder, z_Guides = {
    evars: `./`,
    custEvents: `./`,
    custProps: `./`
}, mpCreds) {

	// // max out memory for mode = fast
	// if (process.env.FAST) {		
	// 	const totalRAM = os.totalmem();
	// 	const totalRamMB = Math.floor(totalRAM/(1024 * 1024));
	// 	const useMostOfIt = Math.floor(totalRamMB * .80)
	// 	process.env['NODE_OPTIONS'] += ` --max-old-space-size=${useMostOfIt}`
	// }

	u.time('total time')
    // FIND ALL THE FILES
    u.time('identify pieces')
    const dataFiles = await u.listFiles(path.resolve(folder));
    const organizedFiles = u.organizeRaw(dataFiles)
    const jobs = Object.keys(organizedFiles);
    const tasks = 0;
    u.time('identify pieces', 'stop')

    console.log(`found ${jobs.length} jobs to do\n`)
    let result = []

    for (const job of jobs) {
        u.time('job time')
		const tasks = organizedFiles[job].raw;
        const lookupComp = organizedFiles[job].lookup
        const totalTasks = tasks.length;
        let currentTask = 1;

		// EXTRACT lookup
		const lookup = await u.extractFile(lookupComp, `lookup`)

        for (const task of tasks) {

            console.log(`doing task ${currentTask} of ${totalTasks} for job ${job}`)
			u.time(`task time`)
			u.time('extract')
			const rawDataPath = await u.extractFile(task)
			u.time('extract', 'stop')

            // PARSE AND CLEAN
            u.time('parse raw')
            const A_metaFiles = await u.listFiles(lookup);
            const B_headers = await u.getHeaders(A_metaFiles.column_headers);
            const C_rawParsed = await u.parseRaw(rawDataPath, B_headers);
            u.time('parse raw', 'stop')


            // PARSE ADOBE LOOKUPS
            u.time('parse lookups')
            const z_metaLookup = await u.getLookups(A_metaFiles);
            const z_evars = (await u.loadTSV(path.resolve(z_Guides.evars), true)).map((infos) => [infos.id.split('/')[1], infos.name])
            const z_custEvents = (await u.loadTSV(path.resolve(z_Guides.custEvents), true)).map((infos) => [infos.id.split('/')[1], infos.name])
            const z_custProps = (await u.loadTSV(path.resolve(z_Guides.custProps), true)).map((infos) => [infos.id.split('/')[1], infos.name])
            const E_enrichedLookups = u.enrichEventList(z_metaLookup, [...z_evars, ...z_custEvents, ...z_custProps]);
            const F_allLookups = {
                ...E_enrichedLookups,
                evars: z_evars,
                custEvent: z_custEvents,
                custProps: z_custProps
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
                recordsPerBatch: 1000, //max # of records in each batch
                bytesPerBatch: 2 * 1024 * 1024, //max # of bytes in each batch
                strict: false, //use strict mode?
                logs: false, //print to stdout?
                //a function reference to be called on every record
                //useful if you need to transform the data
                transformFunc: function noop(a) { return a }
            }
            u.time('flush')
            const importedData = await mpImport(mpCreds, I_transformToMp, options);
            result.push(importedData)
            currentTask++
            u.time('flush', 'stop')
			u.time(`task time`, 'stop')

			//remove the file...
			u.removeFile(rawDataPath)
        }

		//remove the lookup
		u.removeFile(lookup)
		u.time('job time', 'stop')
		console.log('\n')
    }
	
	u.time('total time', 'stop')
	console.log('\n\n')
    return result
}


module.exports = main;