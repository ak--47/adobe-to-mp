// https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-reference.html?lang=en

const path = require('path')
const fs = require('fs').promises
const parseTSV = require('papaparse')
const { zipObject, mapKeys } = require('lodash')
const md5 = require('md5')

exports.listFiles = async function (dir = "./") {
    let fileList = await fs.readdir(dir);
    let results = {};
    for (fileName of fileList) {
        results[fileName.split('.')[0]] = path.resolve(`${dir}/${fileName}`);
    }
    return results
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

exports.applyLookups = async function (raw = [], lookups = {}) {
    const target = clone(raw)
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
        }
    }

    return target;
}

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
            eventName = adobe.post_event_list_resolved[0]
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
        mp = addInsert(mp)
        return mp

    })

    return transform;
}

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