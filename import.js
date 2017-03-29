#!/usr/bin/env node

const fs = require('fs');
const LimitedParallelStream = require('./src/modules/limited_parallel_stream').LimitedParallelStream;
const byline = require('byline'); //@todo replace this module
const riak = require('./riak');
const Riak = require('basho-riak-client');
const program = require('commander');

program
	.version('0.0.1')
	.usage('[options] bucketName')
	.option('-H, --host [host]', 'specify the host (default: http://127.0.0.1)', 'http://127.0.0.1')
	.option('-p, --port [port]', 'specify the post (default: 8098)', 8098)
	.option('-c, --concurrency [concurrency]','specify the concurrency (default: 100)', 100)
	.option('-f, --file [file]', 'specify the folder of dump (default: __dirname + /dump)', __dirname + '/dump')
	.parse(process.argv);

if(!program.args.length) {
	console.log('\nBucket is required!');
	program.help();
}

const bucket = program.args[0];

let stream = fs.createReadStream(program.file);
stream = byline.createStream(stream);

console.info('Import started');

riak.onReady(program.host, program.port, (err, client) =>{
	if(err) {
		throw err
	}

	stream
		.pipe(new LimitedParallelStream(program.concurrency, function(row, enc, done){
			row = row.toString().split('\t');
			const value = new Riak.Commands.KV.RiakObject();
			value.setContentType(row[2]);
			value.setValue(row[1]);

			client.storeValue({
				bucket: bucket,
				key: row[0],
				value: value
			}, (err) =>{
				if(err) {
					return this.emit('error', err)
				}
				done()
			})

		}))
		.on('finish', () =>{
			console.info('\tImport completed ✔');
			process.exit(0);
		});
});