#!/usr/bin/env node

/**
 * @TODO: Move this to dump.js
 */

const request = require('request');
const riak = require('./riak');
const fs = require('fs');
const through2 = require('through2');
const LimitedParallelStream = require('./src/modules/limited_parallel_stream').LimitedParallelStream;
const byline = require('byline');
const program = require('commander');
const listKeys = require('./src/modules/list_keys');
const keysPath = __dirname + '/keys';

program
	.version('0.0.1')
	.usage('[options] bucketName')
	.option('-H, --host [host]', 'specify the host (default: 127.0.0.1)', '127.0.0.1')
	.option('-p, --port [port]', 'specify the post (default: 8098)', 8087)
	.option('-c, --concurrency [concurrency]', 'specify the concurrency (default: 100)', 100)
	.parse(process.argv);

if(!program.args.length) {
	console.error('\nBucket is required!');
	program.help();
}

const bucket = program.args[0];

console.info('Dump started ^_^');
riak.onReady(program.host, program.port, (err, client) =>{
	if(err) {
		throw err
	}

	listKeys.create(client, bucket)
		.pipe(through2.obj(function(chunk, enc, cb){
			chunk = JSON.parse(chunk);

			if(chunk && chunk.length > 0) {
				this.push(chunk.join('\n'));
			}

			cb()
		}))
		.pipe(fs.createWriteStream(keysPath))
		.on('finish', () =>{
			console.info('\tKeys saved on disk ✔');

			//Start saving all data
			byline.createStream(fs.createReadStream(keysPath))
				.pipe(new LimitedParallelStream(program.concurrency, function(key, enc, done){

					client.fetchValue({
						bucket: bucket,
						key: key
					}, (err, data) =>{
						if(err) {
							this.emit('error', err);
							return done()
						}

						if(!data.values) {
							return done();
						}

						const contentType = data.values[0].contentType;
						const value = data.values[0].value.toString();
						this.push(`${key}\t${value}\t${contentType}\n`);
						done()

					});
				}))
				.pipe(fs.createWriteStream(__dirname + '/dump'))
				.on('finish', () =>{
					console.info('\tAll data saved on disk ✔')
				})
				.on('error', (e) =>{
					console.error(e)
				});

		})
		.on('error', (e) =>{
			console.error(e)
		});


});
