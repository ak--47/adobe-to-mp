// https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-reference.html?lang=en

const path = require('path')
const fs = require('fs').promises
const { createReadStream } = require('fs')
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
    const dtrx = execSync(`dtrx --recursive --flat --overwrite ${filePath}`, { cwd: targetDir });

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
    let target = exports.clone(source);
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



exports.time = function (label = `foo`, directive = `start`) {
    if (directive === `start`) {
        console.time(label)
    } else if (directive === `stop`) {
        console.timeEnd(label)
    }

}

exports.clone = function (thing, opts) {
    //don't clone in fast mode
    if (process.env.FAST) {
        return thing;
    }
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