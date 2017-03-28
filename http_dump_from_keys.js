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
	console.log('\nBucket is required!');
	program.help();
}

const bucket = program.args[0];
const baseUrl = `${program.host}:${program.port}/riak/${bucket}/`;

console.info('Dump started ^_^');

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
		console.log(err)
	})
	.on('error', (e) =>{
		console.log(e)
	});