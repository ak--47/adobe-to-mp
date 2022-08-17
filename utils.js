const path = require('path')
const fs = require('fs').promises
const parseTSV = require('papaparse')
const { zipObject } = require('lodash')

exports.listFiles = async function (dir) {
    let fileList = await fs.readdir(dir);
    let results = {};
    for (fileName of fileList) {
        results[fileName.split('.')[0]] = path.resolve(`${dir}/${fileName}`);
    }
    return results
}

exports.getHeaders = async function (filePath) {
    let file = await fs.readFile(filePath, 'utf8');
    let headers = parseTSV.parse(file).data[0];
    return headers;
}

exports.parseRaw = async function (rawPath, headers) {
    let rawData = await fs.readFile(rawPath, 'utf-8');
    let parseRaw = parseTSV.parse(rawData).data;
	let labeled = parseRaw.map((row)=>{
		return zipObject(headers, row)
	})
    return labeled;
}

exports.getLookups = async function(metas = {}) {
	//don't need col headers; already got 'em
	delete metas.column_headers;

	let result = {};		
	for (let key in metas) {
		let lookup = parseTSV.parse(await fs.readFile(metas[key], 'utf-8')).data
		result[key] = lookup
	}

	return result
}
