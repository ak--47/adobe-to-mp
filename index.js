const u = require('./utils.js');
const p = require('./pipeline.js')
const path = require('path');
const fs = require('fs').promises
const mpImport = require('mixpanel-import');
const os = require('os')
const { chain } = require('stream-chain');



async function main(folder, z_Guides = {
    evars: `./`,
    custEvents: `./`,
    custProps: `./`
}, mpCreds) {

    // max out memory for mode = fast
    if (process.env.FAST) {
        console.log(`fast mode enabled!`)
        const totalRAM = os.totalmem();
        const totalRamMB = Math.floor(totalRAM/(1024 * 1024));
        const useMostOfIt = Math.floor(totalRamMB * .90)
		if (!process.env['NODE_OPTIONS']?.includes('--max-old-space-size')) {
			console.log(`setting memory to ${totalRamMB} MB`)
			process.env['NODE_OPTIONS'] += ` --max-old-space-size=${useMostOfIt}`
		}
        
    }

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



            // PARSE LOOKUPS AND CLEAN
            u.time('parse lookups')
            const standMetaFiles = await u.listFiles(lookup);
            const colHeaders = await u.getHeaders(standMetaFiles.column_headers);
            const standardLookups = await u.getLookups(standMetaFiles);
            const evars = (await u.loadTSV(path.resolve(z_Guides.evars), true)).map((infos) => [infos.id.split('/')[1], infos.name])
            const custEventLabels = (await u.loadTSV(path.resolve(z_Guides.custEvents), true)).map((infos) => [infos.id.split('/')[1], infos.name])
            const custPropLabels = (await u.loadTSV(path.resolve(z_Guides.custProps), true)).map((infos) => [infos.id.split('/')[1], infos.name])
            const enrichedLookups = u.enrichEventList(standardLookups, [...evars, ...custEventLabels, ...custPropLabels]);
            const allLookups = {
                ...enrichedLookups,
                evars: evars,
                custEvent: custEventLabels,
                custProps: custPropLabels
            }
            u.time('parse lookups', 'stop')


            // each 'task' is a chained pipeline using the extracted data
            // https://www.npmjs.com/package/stream-chain
			
            u.time('parse raw')
            const rawParsedSourceData = await p.parseRaw(rawDataPath, colHeaders);
            u.time('parse raw', 'stop')

            // APPLY ADOBE LOOKUPS
            u.time('apply lookups to raw')
            const dataWithLookups = p.applyLookups(rawParsedSourceData, allLookups);
            const cleanedSourceData = p.noNulls(dataWithLookups);
            u.time('apply lookups to raw', 'stop')
            
			//TRANSFORM AND SEND TO MP
            u.time('transform to mp')
            const mixpanelFormat = p.adobeToMp(cleanedSourceData)
            u.time('transform to mp', 'stop')

            const mpOptions = {
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
            const importedData = await mpImport(mpCreds, mixpanelFormat, mpOptions);
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