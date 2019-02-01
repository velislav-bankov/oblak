'use strict';

const fs = require('fs');

const loadBootstrap = require('../loadBootstrap');
const EventstoreStore = require('../../plugins/shared/Eventstore/index');

const { pipeline, JSONLinesStringify, CounterStream } = require('../../utils/stream.js');

const loadOblak = (konzola) => {
	const stop = konzola.wait({ text: 'Loading Oblak Bootstrap...' });
	const oblak = loadBootstrap();
	oblak._load();
	stop();
	process.stdout.clearLine();
	konzola.success('Oblak Bootstrap Loaded');
	return oblak;
};

const initModule = async (module, konzola, moduleName) => {
	const stop = konzola.wait({ text: `Initializing ${moduleName}...` });
	await module.init();
	stop();
	process.stdout.clearLine();
	konzola.success(`${moduleName} Initilized`);
	return module;
};


const info = {
	description: 'Export Oblak Data ( database )',

	async getOptionDefinitions() {
		return [
			{
				name: 'out',
				alias: 'o',
				type: String,
				description: 'Output filename',
				defaultValue: './eventstore.export.jsonlines',
				typeLabel: '<out>',
			},
		];
	},

	async run({ konzola }, arg) {
		const oblak = loadOblak(konzola, arg);
		const kellner = await initModule(oblak._kellner, konzola, 'Kellner');
		const store = await initModule(new EventstoreStore(kellner), konzola, 'Eventstore');

		const counterStream = new CounterStream();
		const stop = konzola.wait({ text: 'Exporting events...' });
		await pipeline(
			store.getExportStream(),
			counterStream,
			new JSONLinesStringify(),
			fs.createWriteStream(arg.out, { encoding: 'utf8' }),
		);
		konzola.success(`${counterStream.counter} event(s) exported.`);
		stop();
		process.exit(0);
	},
};

module.exports = info;
