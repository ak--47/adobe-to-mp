// https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-reference.html?lang=en

const path = require('path')
const fs = require('fs').promises
const parseTSV = require('papaparse')
const { zipObject, mapKeys, find } = require('lodash')
const md5 = require('md5')
const { execSync } = require("child_process")
const makeDir = require('fs').mkdirSync


exports.listFiles = async function (dir = "./") {
    let fileList = await fs.readdir(dir);
    let results = {};
    for (fileName of fileList) {
        results[fileName.split('.')[0]] = path.resolve(`${dir}/${fileName}`);
    }
    return results
}

exports.organizeRaw = function (fileMapping) {
    const filenames = Object.values(fileMapping);
    const dataFiles = filenames.filter(f => f.endsWith('.tsv.gz'));
    const lookups = filenames.filter(f => f.endsWith('lookup_data.tar.gz'));

    //organize raw files
    const organized = {};
    for (const filePath of dataFiles) {
        const suffix = filePath.split('.tsv.gz')[0].split('-').slice(-1);
        if (organized[suffix]) {
            organized[suffix].raw.push(filePath)
        } else {
            organized[suffix] = {
                raw: [],
                lookup: ``
            }

            organized[suffix].raw.push(filePath)
        }
    }

    //add lookups
    for (const filePath of lookups) {
        const suffix = filePath.split('-lookup_data.tar.gz')[0].split('-').slice(-1)
        organized[suffix].lookup = filePath
    }

    return organized

}

exports.extractFile = async function (filePath, subDir = "") {
    const uid = process.getuid()
    const gid = process.getgid()
    const targetDir = path.resolve(`./tmp/${subDir}`);
    try {
        const newDir = makeDir(targetDir, 0777);
    } catch (err) {
        if (err.code !== 'EEXIST') {
            throw err;
        }
    }
    let target;
    if (subDir) {
        target = targetDir
    } else {
        target = path.resolve(`${targetDir}/${filePath.split('.gz')[0].split("/").slice(-1)}`)
    }

    // https://manpages.ubuntu.com/manpages/xenial/man1/dtrx.1.html
    const dtrx = execSync(`dtrx --recursive --flat --overwrite ${filePath}`, {cwd: targetDir});

    return target

}

exports.removeFile = function (fileNameOrPath) {
	const removed = execSync(`rm -rf ${fileNameOrPath}`)
	return true;
}

exports.getHeaders = async function (filePath = "./") {
    let file = await fs.readFile(filePath, 'utf8');
    let headers = parseTSV.parse(file).data[0];
    return headers;
}

exports.parseRaw = async function (rawPath = "./", headers = []) {
    let rawData = await fs.readFile(rawPath, 'utf-8');
    let parseRaw = parseTSV.parse(rawData).data;
    let labeled = parseRaw.map((row) => {
        return zipObject(headers, row)
    })
    return labeled;
}

exports.getLookups = async function (metas = {}) {
    //don't need col headers; already got 'em
    delete metas.column_headers;

    let result = {};
    for (let key in metas) {
        let lookup = parseTSV.parse(await fs.readFile(metas[key], 'utf-8')).data
        result[key] = lookup
    }

    //hacky shit i found when spot checking; lookups + raw do not share named keys
    result.search_engine = result.search_engines
    delete result.search_engines

    result.ref_type = result.referrer_type
    delete result.referrer_type

    result.os = result.operating_systems
    delete result.operating_systems

    result.language = result.languages
    delete result.languages

    result.javascript = result.javascript_version
    delete result.javascript_version

    result.event_list = result.event
    delete result.event

    //moar hacky shit for transformation resolution
    const viewports = result.resolution
    result.browser_width = viewports.map((pair) => [pair[0], pair[1]?.split('x')?.[0]?.trim() || pair[1]]);
    result.browser_height = viewports.map((pair) => [pair[0], pair[1]?.split('x')?.[1]?.trim() || pair[1]]);
    delete result.resolution


    return result
}

exports.loadTSV = async function (filePath = "", hasHeaders = false) {
    let file = await fs.readFile(filePath, 'utf8');
    return parseTSV.parse(file, { header: hasHeaders }).data;
}

exports.enrichEventList = function (source, enrich) {
    let target = clone(source);
    for (let dim of target.event_list) {
        if (dim.length === 2) {
            let searchValue = dim[1]?.trim()?.toLowerCase()?.replace(/\s/g, '')?.replace("instanceof", "")?.replace("customevent", "event");

            let searchResults = find(enrich, function (value, index, collection) {
                return value[0] === searchValue
            })

            if (searchResults) {
                dim[1] = searchResults[1]
            }

        }


    }


    return target

}

exports.applyLookups = function (raw = [], lookups = {}) {
    const target = clone(raw)

    // evar lookup
    const evarKeyMap = {};
    for (const evar of lookups.evars) {
        evarKeyMap[`post_${evar[0]}`] = evar[1]
    }

    // prop lookup
    const propKeyMap = {};
    for (const prop of lookups.custProps) {
        propKeyMap[`post_${prop[0]}`] = prop[1]
    }

    for (const event of target) {
        for (let key in event) {
            if (lookups[key]) {
                let ogValue = event[key];
                let pair = lookups[key].filter(pair => pair[0] === ogValue).flat()
                if (pair.length > 0) {
                    let newValue = pair[1]
                    event[key] = newValue
                }
            }

            //resolve event names
            if (key === `post_event_list` && event[key]) {
                const allEventsInSession = event[key].split(',');
                const eventNameLookup = allEventsInSession.map((evId) => {
                    const resolvedEvent = lookups.event_list.filter(pair => pair[0] === evId).flat()[1]
                    return resolvedEvent
                })

                event.post_event_list_resolved = eventNameLookup;
            }

            //resolve evars
            if (key.includes('post_evar')) {
                //evars tend to be JSON
                if (event[key]?.startsWith('{') && event[key]?.endsWith('}')) {
                    try {
                        event[evarKeyMap[key]] = JSON.parse(event[key])
                    } catch (e) {
                        event[evarKeyMap[key]] = event[key]
                    }
                } else {
                    event[evarKeyMap[key]] = event[key]
                }

                delete event[key]
            }

            //resolve props
            if (key.includes('post_prop')) {
                event[propKeyMap[key]] = event[key]
                delete event[key]
            }
        }
    }

    return target;
}

//no longer used
exports.applyEvars = async function (raw = [], evars) {
    //rename the evar keys
    const keyMap = {};
    for (const evar of evars) {
        keyMap[`post_${evar.id.split('/')[1]}`] = evar.name
    }

    const target = clone(raw)

    const appliedEvars = target.map((event) => {
        return mapKeys(event, (value, key) => {
            if (keyMap[key]) {
                return keyMap[key]
            } else {
                return key
            }
        })
    })

    return appliedEvars;
}

exports.noNulls = function (arr = []) {
    let target = clone(arr)
    for (let thing of target) {
        removeNulls(thing)
    }

    return target
}

exports.adobeToMp = function (events = []) {
    const transform = events.map((adobe) => {
        //this is garbage
        let eventName;
        try {
            eventName = `hit`
        } catch (e) {
            eventName = `unknown`
        }

        let mp = {
            event: eventName,
            properties: {
                time: Number(adobe.visit_start_time_gmt),
                distinct_id: `${adobe.post_visid_high}${adobe.post_visid_low}`,
                ...adobe
            }
        }
        // $insert_id
        // mp = addInsert(mp)
        return mp

    })

    return transform;
}

exports.time = function (label = `foo`, directive = `start`) {
    if (directive === `start`) {
        console.time(label)
    } else if (directive === `stop`) {
        console.timeEnd(label)
    }

}

//LOCAL
const addInsert = function (event) {
    let hash = md5(event);
    event.properties.$insert_id = hash;
    return event
}

const removeNulls = function (obj) {
    function isObject(val) {
        if (val === null) { return false; }
        return ((typeof val === 'function') || (typeof val === 'object'));
    }

    const isArray = obj instanceof Array;

    for (var k in obj) {
        // falsy values
        if (!Boolean(obj[k])) {
            isArray ? obj.splice(k, 1) : delete obj[k]
        }

        //empty strings
        if (obj[k] === "") {
            delete obj[k]
        }

        // empty arrays
        if (Array.isArray(obj[k]) && obj[k]?.length === 0) {
            delete obj[k]
        }

        // empty objects
        if (isObject(obj[k])) {
            if (JSON.stringify(obj[k]) === '{}') {
                delete obj[k]
            }
        }

        // recursion
        if (isObject(obj[k])) {
            removeNulls(obj[k])
        }
    }
}

const clone = function (thing, opts) {
    var newObject = {};
    if (thing instanceof Array) {
        return thing.map(function (i) { return clone(i, opts); });
    } else if (thing instanceof Date) {
        return new Date(thing);
    } else if (thing instanceof RegExp) {
        return new RegExp(thing);
    } else if (thing instanceof Function) {
        return opts && opts.newFns ?
            new Function('return ' + thing.toString())() :
            thing;
    } else if (thing instanceof Object) {
        Object.keys(thing).forEach(function (key) {
            newObject[key] = clone(thing[key], opts);
        });
        return newObject;
    } else if ([undefined, null].indexOf(thing) > -1) {
        return thing;
    } else {
        if (thing.constructor.name === 'Symbol') {
            return Symbol(thing.toString()
                .replace(/^Symbol\(/, '')
                .slice(0, -1));
        }
        // return _.clone(thing);  // If you must use _ ;)
        return thing.__proto__.constructor(thing);
    }
}