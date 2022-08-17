const u = require('./utils.js');
const path = require('path');
const fs = require('fs').promises

async function main(z_rawFile, z_metaDatDir, z_eVarDfns) {	
	const A_metaFiles = await u.listFiles((path.resolve(z_metaDatDir)));
	const B_headers = await u.getHeaders(A_metaFiles.column_headers);
	const C_rawParsed = await u.parseRaw(path.resolve(z_rawFile), B_headers);
	const D_metaLookup = await u.getLookups(A_metaFiles);
	const E_joinMetaAndRaw = await u.applyLookups(C_rawParsed, D_metaLookup);
	const F_evarSource = await u.loadTSV(path.resolve(z_eVarDfns), true);
	const G_dataWithEvar = await u.applyEvars(E_joinMetaAndRaw, F_evarSource);
	const H_dataSansEmpties = u.noNulls(G_dataWithEvar)

	debugger;
	return 42
}


module.exports = main;