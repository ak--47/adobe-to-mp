
import functions from '@google-cloud/functions-framework';
import u from 'ak-tools';
import path from 'path';
import { log } from './logger.js';

/*
----
CACHING STRATEGY
----
*/

// Lazy-loaded modules to prevent blocking startup
let mainFunction = null;
let aggregatedGuides = null;

async function getMain() {
	if (!mainFunction) {
		log.info('Loading main transform function...');
		const indexModule = await import('./index.js');
		mainFunction = indexModule.default;
		log.info('Main transform function loaded');
	}
	return mainFunction;
}

async function getAggregatedGuides() {
	if (!aggregatedGuides) {
		log.info('Loading aggregated guides...');
		const guidesModule = await import('./korn-ferry-guides.js');
		aggregatedGuides = guidesModule.AGGREGATED_GUIDES;
		log.info('Aggregated guides loaded');
	}
	return aggregatedGuides;
}

/*
----
CLOUD ENTRY
----
*/

functions.http('start', async (req, res) => {
	try {
		log.info({ body: req.body }, "REQUEST RECEIVED");
		const { cloud_path, dest_path } = req.body;

		if (!cloud_path || !dest_path) {
			log.error({ body: req.body }, "Missing required parameters");
			return res.status(400).send({ error: "cloud_path and dest_path are required" });
		}

		log.info({ body: req.body }, "TRANSFORM START");

		// Lazy load dependencies in parallel
		const [main, guides] = await Promise.all([
			getMain(),
			getAggregatedGuides()
		]);

		const { human, delta, source, destination, ...results } = await main(cloud_path, dest_path, guides);
		log.info({ source, destination, elapsed: delta, ...req.body, ...results }, `TRANSFORM END: ${human}`);
		res.status(200).send({ status: "OK" });
	} catch (e) {
		log.error({ error: e, body: req.body }, "ERROR!");
		res.status(500).send(e);
	}
});

