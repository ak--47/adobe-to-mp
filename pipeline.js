const path = require('path')
const { createReadStream } = require('fs')
const parseTSV = require('papaparse')
const { zipObject } = require('lodash')
const md5 = require('md5')
const { clone } = require('./utils.js')


exports.parseRaw = async function (rawPath = "./", headers = []) {

    return new Promise((resolve, reject) => {
        let rawStream = createReadStream(rawPath, 'utf-8');
        let parseRaw = [];
        let parseStream = parseTSV.parse(rawStream, {
            step: function (results, parser) {
                chunk(results, parser);
            },
            complete: function (results, file) {
                resolve(finish(results, file))
            }
        });

        function chunk(results, parser) {
            parseRaw.push(results.data)
        }


        function finish() {
            let labeled = parseRaw.map((row) => {
                return zipObject(headers, row)
            })
            return labeled;
        }

    })
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


exports.noNulls = function (arr = []) {
    let target = clone(arr)
    for (let thing of target) {
        removeNulls(thing)
    }

    return target
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