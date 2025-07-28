
// const functions = require('@google-cloud/functions-framework');
import functions from '@google-cloud/functions-framework';
import u from 'ak-tools';
import path from 'path';
import { main, log } from './index.js';

/*
----
CLOUD ENTRY
----
*/


functions.http('start', async (req, res) => {
	try {
		const sourceFile = getFileName(req.body.cloud_path);
		log.info({ file: sourceFile, ...req.body }, "TRANSFORM START");
		const { cloud_path, dest_path } = req.body;
		const { human, delta } = await main(cloud_path, dest_path);
		log.info({ file: sourceFile, elapsed: delta, ...req.body }, `TRANSFORM END: ${human}`);
		res.status(200).send({ status: "OK" });
	} catch (e) {
		log.error({ error: e, body: req.body }, "ERROR!");
		res.status(500).send(e);
	}
});


function getFileName(cloud_path) {
	const { bucket, file: cloudURI } = u.parseGCSUri(cloud_path);
	const filename = path.basename(cloud_path);
	return filename;
}