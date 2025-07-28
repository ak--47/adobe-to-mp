
// const functions = require('@google-cloud/functions-framework');
import functions from '@google-cloud/functions-framework';
import u from 'ak-tools';
import path from 'path';
import main, { log } from './index.js';
import { AGGREGATED_GUIDES } from './korn-ferry-guides.js';

/*
----
CLOUD ENTRY
----
*/


functions.http('start', async (req, res) => {
	try {
		log.info({ ...req.body }, "TRANSFORM START");
		const { cloud_path, dest_path } = req.body;
		const { human, delta, source, destination, ...results } = await main(cloud_path, dest_path, AGGREGATED_GUIDES);
		log.info({ source, destination, elapsed: delta, ...req.body, ...results }, `TRANSFORM END: ${human}`);
		res.status(200).send({ status: "OK" });
	} catch (e) {
		log.error({ error: e, body: req.body }, "ERROR!");
		res.status(500).send(e);
	}
});



