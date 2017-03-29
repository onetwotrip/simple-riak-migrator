#!/usr/bin/env node

const fs = require('fs');
const LimitedParallelStream = require('./src/modules/limited_parallel_stream').LimitedParallelStream;
const byline = require('byline'); //@todo replace this module
const program = require('commander');
const request = require('requestretry');

program
	.version('0.0.1')
	.usage('[options] bucketName')
	.option('-H, --host [host]', 'specify the host (default: http://127.0.0.1)', 'http://127.0.0.1')
	.option('-p, --port [port]', 'specify the post (default: 8098)', 8098)
	.option('-c, --concurrency [concurrency]', 'specify the concurrency (default: 100)', 100)
	.option('-f, --file [file]', 'specify the folder of dump (default: __dirname + /dump)', __dirname + '/dump')
	.parse(process.argv);

if(!program.args.length) {
	console.log('\nBucket is required!');
	program.help();
}

const bucket = program.args[0];
console.info('Import started');


byline.createStream(fs.createReadStream(program.file))
	.pipe(new LimitedParallelStream(program.concurrency, function(row, enc, done){
		row = row.toString().split('\t');
		const key = row[0];
		const value = row[1];
		const contentType = row[2];


		request({
			url: `${program.host}:${program.port}/${bucket}/${key}`,
			method: 'PUT',
			headers: {
				'Content-Type': contentType
			},
			body: value
		})
			.then(() => done())
			.catch(e => console.log(e));
	}))
	.on('error', (e) =>{
		console.log(e);
	})
	.on('finish', () =>{
		console.info('\tImport completed ✔');
		process.exit(0);
	});