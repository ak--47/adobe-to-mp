const path = require('path')
const { createReadStream } = require('fs')
const parseTSV = require('papaparse')
const { zipObject } = require('lodash')
const md5 = require('md5')
const { clone } = require('./utils.js')


exports.parseRaw = function (rawStream) {
    return parseTSV.parse(rawStream);

    // return new Promise((resolve, reject) => {
    //     let rawStream = createReadStream(rawPath, 'utf-8');
    //     //let parseRaw = [];
    //     let parseStream = parseTSV.parse(rawStream, {
    //         step: function (results, parser) {
    //             //chunk(results, parser);
    // 			return zipObject(headers, results)
    //         },
    //         complete: function (results, file) {
    //             // resolve(finish(results, file))
    // 			resolve(results)
    //         }
    //     });

    //     function chunk(results, parser) {
    //         parseRaw.push(results.data)
    //     }


    //     function finish() {
    //         let labeled = parseRaw.map((row) => {
    //             return zipObject(headers, row)
    //         })
    //         return labeled;
    //     }

    // })
}

exports.applyHeaders = function (stream, headers) {
    return zipObject(headers, stream)
}

exports.applyLookups = function (stream, lookups = {}) {
    const event = stream

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


    return event;
}

exports.adobeToMp = function (adobe = {}) {

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
            time: Number(adobe.hit_time_gmt) || Number(adobe.cust_hit_time_gmt) || Date.now(),
            distinct_id: `${adobe.post_visid_high}${adobe.post_visid_low}`,
            ...adobe
        }
    }
    // $insert_id
    // mp = addInsert(mp)
	// if (isNaN(mp.properties.time)) {
	// 	debugger;
	// }
    return mp

}


exports.noNulls = function (arr = []) {
    let target = clone(arr)
    for (let thing of target) {
        removeNulls(thing)
    }

    return target
}

exports.cleanObject = function (obj) {
    let target = JSON.parse(JSON.stringify(obj))

    function isObject(val) {
        if (val === null) { return false; }
        return ((typeof val === 'function') || (typeof val === 'object'));
    }

    const isArray = target instanceof Array;

    for (var k in target) {
        // falsy values
        if (!Boolean(target[k])) {
            isArray ? target.splice(k, 1) : delete target[k]
        }

        //empty strings
        if (target[k] === "") {
            delete target[k]
        }

        // empty arrays
        if (Array.isArray(target[k]) && target[k]?.length === 0) {
            delete target[k]
        }

        // empty objects
        if (isObject(target[k])) {
            if (JSON.stringify(target[k]) === '{}') {
                delete target[k]
            }
        }

        // recursion
        if (isObject(target[k])) {
            exports.cleanObject(target[k])
        }
    }

    return target
}

//LOCAL
const addInsert = function (event) {
    let hash = md5(event);
    event.properties.$insert_id = hash;
    return event
}