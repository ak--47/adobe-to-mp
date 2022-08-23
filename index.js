const u = require('./utils.js');
const p = require('./pipeline.js')
const path = require('path');
const fs = require('fs').promises
const { createReadStream } = require('fs')
const mpImport = require('mixpanel-import');
const os = require('os')
const chain = require('stream-chain');
const Batch = require('stream-json/utils/Batch');
const split = require('split2')


async function main(folder, z_Guides = {
    evars: `./`,
    custEvents: `./`,
    custProps: `./`
}, mpCreds) {

    // max out memory for mode = fast
    if (process.env.FAST) {
        console.log(`fast mode enabled!`)
        const totalRAM = os.totalmem();
        const totalRamMB = Math.floor(totalRAM / (1024 * 1024));
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

    loopJobs: for (const job of jobs) {
        u.time('job time')
        const tasks = organizedFiles[job].raw;
        const lookupComp = organizedFiles[job].lookup
        const totalTasks = tasks.length;
        let currentTask = 1;

        // EXTRACT lookup
        const lookup = await u.extractFile(lookupComp, `lookup`)

        loopTasks: for (const task of tasks) {

            console.log(`doing task ${currentTask} of ${totalTasks} for job ${job}`)
            // u.time(`task time`)
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

            const mpOptions = {
                recordType: `event`, //event, user, OR group
                streamSize: 27, // highWaterMark for streaming chunks (2^27 ~= 134MB)
                region: `US`, //US or EU
                recordsPerBatch: 1000, //max # of records in each batch
                bytesPerBatch: 2 * 1024 * 1024, //max # of bytes in each batch
                strict: false, //use strict mode?
                logs: false, //print to stdout?
                streamFormat: 'json',
                //a function reference to be called on every record
                //useful if you need to transform the data
                transformFunc: function noop(a) { return a }
            }


            let rawStream = createReadStream(rawDataPath, { encoding: 'utf-8' });
            let pipeline = await orchestratePipeline(rawStream, rawDataPath, colHeaders, allLookups, mpOptions, mpCreds);

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


async function orchestratePipeline(stream, rawDataPath, colHeaders, allLookups, mpOptions, mpCreds) {
    u.time(`task time`)
    let responses = [];
    return new Promise((resolve, reject) => {
        // each 'task' is a chained pipeline using the extracted data
        // https://www.npmjs.com/package/stream-chain
        const etl = new chain([
            (stream) => { return p.parseRaw(stream).data },
            (data) => { return p.applyHeaders(data, colHeaders) },
            (data) => { return p.applyLookups(data, allLookups) },
            (data) => { return p.cleanObject(data) },
            (data) => { return p.adobeToMp(data) },
            new Batch({ batchSize: 1000 }),
            async (batch) => {
                return await mpImport(mpCreds, batch, mpOptions)
            }
        ])

        etl.on('error', (error) => {
            console.log(error)
            reject(error)
        });

        etl.on('data', (res, f, o) => {
            responses.push(res.responses)
        })

        etl.on('end', (res) => {
            u.time(`task time`, `stop`)
            //remove the file
            u.removeFile(rawDataPath)
            resolve(responses)
        });

        stream.pipe(split()).pipe(etl);
    })

}

module.exports = main;