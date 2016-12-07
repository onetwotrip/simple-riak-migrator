#!/usr/bin/env node

var program = require('commander');
var async = require('async');
var request = require('request');
var url = require('url');

program
    .version('0.0.8')
    .usage('[options] bucketName')
    .option('-H, --host [host]','specify the host (default: localhost)')
    .option('-p, --port [port]','specify the post (default: 8098)')
    .option('-f, --file [FileName]','specify the file name (default: [bucket].json)')
    .option('-i, --import','import mode (instead of reading from bucket entries will be written to bucket)')
    .option('-c, --concurrency [concurrency]','specify the concurrency (default: 20)')
    .option('-m, --meta [meta]', 'import with meta (default: False)')
    .option('-P, --pretty [pretty]', 'pretty stringify of json (default: False)')
    .option('--delete', 'delete the keys as they are exported (DANGER: possible data loss)')
    .parse(process.argv);
if(!program.args.length) {
    program.help();
}
var bucket = program.args;
program.host = program.host || 'localhost';
program.port = program.port || '8098';
program.file = program.file || bucket+'.json';
program.concurrency = !isNaN(program.concurrency) ? parseInt(program.concurrency, 10) : 20;
program.meta = (program.meta==='true')  || false;
program.pretty = (program.pretty==='true') || false;
var count = 0;
var openWrites = 0;
var fs = require('fs');
var riakUrl = 'http://' + program.host + ":" + program.port + '/riak';

if (program.import) {
    importToBucket();
} else {
    if (program.delete) {
        console.log('WARNING: keys will be deleted as they are exported');
    }

    exportFromBucket();
}

function importToBucket() {
    if (!fs.existsSync(program.file)) {
        throw new Error('the import file does not exist');
    }

    fs.readFile(program.file, 'utf8', function (err,data) {
        if (err) {
            return console.log(err);
        }
        var entries = data.split('\r\n');
        async.eachLimit(entries, program.concurrency, function(entry, cb) {
            try{
                if(!entry || !entry.trim()){
                    return cb();
                }

                entry = JSON.parse(entry);

                if(typeof entry.data !== typeof {} && typeof entry.data != 'number' ){
                    entry.data = new Buffer(entry.data, 'base64');
                    entry.headers['content-encoding'] = entry.headers['content-encoding'] || 'gzip';
                }
                else{
                    entry.data = JSON.stringify(entry.data);
                }
            }
            catch(err){
                return cb(err);
            }

            var keyUrl = createKey(entry.key);

            console.log('inserting entry with key %j', entry.key);

            request(keyUrl, {
                method: 'PUT',
                headers: entry.headers,
                body: entry.data
            }, function(err, response){
                if(err || (response && response.statusCode !== 204)){
                    console.log(err, response && response.statusCode);
                    return cb(err || (response && response.statusCode));
                }
                else{
                    return cb();
                }
            });
        }, function(err) {
            if (err) {
                return console.log(err);
            }
            return console.log('%j entries inserted into bucket %j', entries.length, bucket);
        });
    });
}

var receivedAll = false;
var q = async.queue(processKey, program.concurrency);
q.drain = end;

function exportFromBucket() {
    if (fs.existsSync(program.file)) {
        throw new Error('the output file already exists');
    }
    console.log('fetching bucket '+bucket+' from '+program.host+':'+program.port);
    var bucketKeysUrl = [riakUrl, bucket].join('/') + '?keys=stream';
    console.log(bucketKeysUrl);
    request(bucketKeysUrl, function(err){
        if (err) {
            console.log('failed to fetch keys');
            console.log(err);
        }
    }).on('data', function(data) {
        // decompressed data as it is received
        var parsedData = isValidJSON(data);
        if(parsedData && parsedData.keys){
            handleKeys(parsedData.keys);
        }
    }).on('end', function(){
        console.log('received all keys');
        receivedAll = true;
    });
}

function end() {
    if (!receivedAll) {
        return;
    }
    if (count<=0) {
        console.log('nothing exported');
    } else {
        console.log('finished export of '+count+' keys to '+program.file);
    }
}

function handleKeys(keys) {
    count+=keys.length;
    for (var i=0;i<keys.length;i++) {
        var key = keys[i];
        openWrites++;
        q.push(key, function() {
            openWrites--;
        });
    }
    console.log('queue size: ' + q.length());
}

var isValidJSON = function(data){
    try{
        return JSON.parse(data);
    }
    catch(err){
        return false
    }
};

var createKey = function(key){
    return [riakUrl, bucket, key].join('/');
};

function processKey(key, cb) {
    console.log('exporting key ' + key);
    var keyUrl = createKey(key);

    request(keyUrl, {encoding: null}, function(err, response, body){
        if(err || response.statusCode !== 200){
            console.log('ERROR', err, response && response.statusCode, keyUrl);
            return cb();
        }

        var out = {
            key: key,
            headers: response.headers
        };

        var data = isValidJSON(body);

        if(data){
            out.data = data;
        }
        else{
            out.data = body.toString('base64');
        }

        var options = [out];

        if(program.pretty){
            options = options.concat([null, '\t']);
        }

        fs.appendFileSync(program.file, JSON.stringify.apply(this, options) + '\r\n');

        return cb();
    });
}
