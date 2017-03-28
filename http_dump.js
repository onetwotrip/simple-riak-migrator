#!/usr/bin/env node

const request = require('request');
const fs = require('fs');
const through2 = require('through2');
const keysPath = __dirname + '/keys';
const LimitedParallelStream = require('./src/modules/limited_parallel_stream').LimitedParallelStream;
const byline = require('byline');
const program = require('commander');

program
	.version('0.0.1')
	.usage('[options] bucketName')
	.option('-H, --host [host]', 'specify the host (default: http://127.0.0.1)', 'http://127.0.0.1')
	.option('-p, --port [port]', 'specify the post (default: 8098)', 8098)
	.option('-c, --concurrency [concurrency]', 'specify the concurrency (default: 100)', 100)
	.parse(process.argv);

if(!program.args.length) {
	console.error('\nBucket is required!');
	program.help();
}

const bucket = program.args[0];
const baseUrl = `${program.host}:${program.port}/riak/${bucket}/`;

/**
 * @TODO: Move this to dump.js
 */

console.info('Dump started ^_^');
request(`${baseUrl}?keys=stream`)
	.pipe(through2.obj(function(chunk, enc, cb){
		let data;
		try{
			data = JSON.parse(chunk.toString());
		}
		catch(e) {
			console.error('Not valid JSON', chunk.toString());
			return cb();
		}

		if(data.keys && data.keys.length > 0) {
			this.push(data.keys.join('\n') + '\n');
		}

		cb()
	}))
	.pipe(fs.createWriteStream(keysPath))
	.on('finish', () =>{
		console.info('\tKeys saved on disk ✔');

		//Start saving all data
		let stream = fs.createReadStream(keysPath);
		stream = byline.createStream(stream);
		stream
			.pipe(new LimitedParallelStream(program.concurrency, function(key, enc, done){
				const url = `${baseUrl}${key}`;
				request(url, (err, data) =>{
					if(!err && data.statusCode === 200) {
						this.push(`${key}\t${data.body}\t${data.headers['content-type']}\n`);
					} else {
						this.emit('error', err)
					}
					done()
				});
			}))
			.pipe(fs.createWriteStream(__dirname + '/dump'))
			.on('finish', () =>{
				console.info('\tAll data saved on disk ✔')
			})
			.on('err', (err) =>{
				console.error(err)
			})

	})
	.on('error', (e) =>{
		console.error(e)
	});