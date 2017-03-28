#!/usr/bin/env node

const request = require('request');
const program = require('commander');
const fs = require('fs');
const through2 = require('through2');
const keysPath = __dirname + '/keys';
const byline = require('byline');

program
	.version('0.0.1')
	.usage('[options] bucketName')
	.option('-H, --host [host]', 'specify the host (default: http://127.0.0.1)', 'http://127.0.0.1')
	.option('-p, --port [port]', 'specify the post (default: 8098)', 8098)
	.parse(process.argv);

if(!program.args.length) {
	console.log('\nBucket is required!');
	program.help();
}

const bucket = program.args[0];
const baseUrl = `${program.host}:${program.port}/riak/`;

console.info('Dump keys started ^_^');

request(baseUrl + bucket + '/?keys=stream')
	.pipe(through2.obj(function(chunk, enc, cb){
		const data = JSON.parse(chunk.toString());
		if(data.keys && data.keys.length > 0) {
			this.push(data.keys.join('\n') + '\n');
		}

		cb()
	}))
	.pipe(fs.createWriteStream(keysPath))
	.on('finish', () =>{
		console.info('\tKeys saved on disk ✔');

	})
	.on('error', (e) =>{
		console.log(e)
	});

