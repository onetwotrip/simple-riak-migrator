#!/usr/bin/env node
const program = require('commander');
const async = require('async');
const request = require('request');
const fs = require('fs');
const mergeWith = require('lodash.mergewith');

program
  .version('0.1.0')
  .usage('[options] bucketName')
  .option('-H, --host [host]', 'specify the host (default: localhost)')
  .option('-p, --port [port]', 'specify the post (default: 8098)')
  .option('-f, --file [FileName]', 'specify the file name (default: [bucket].json)')
  .option('-i, --import', 'import mode (instead of reading from bucket entries will be written to bucket)')
  .option('-c, --concurrency [concurrency]', 'specify the concurrency (default: 20)')
  .parse(process.argv);

if (!program.args.length) {
  program.help();
}

let count = 0;
let openWrites = 0;
let receivedAll = false;
const bucket = program.args;
program.host = program.host || 'localhost';
program.port = program.port || '8098';
program.file = program.file || `${bucket}.json`;
program.concurrency = !isNaN(program.concurrency) ? parseInt(program.concurrency, 10) : 20;

const riakUrl = `http://${program.host}:${program.port}/riak`;

const isValidJSON = (data) => {
  try {
    return JSON.parse(data);
  } catch (err) {
    return false;
  }
};

const createKey = (key) => {
  return [riakUrl, bucket, encodeURIComponent(key)].join('/');
};

function processKey(key, cb) {
  console.log('[INFO]:', `exporting key ${key}`);
  const keyUrl = createKey(key);

  request(keyUrl, { encoding: null }, function (err, response, body) {
    if (err || response.statusCode !== 200) {
      console.log('[ERROR]:', err, response.statusCode, keyUrl);
      return cb();
    }

    const entry = `${JSON.stringify({
      key,
      headers: response.headers,
      data: isValidJSON(body) || body.toString('base64'),
      v: '4be4152cd7194cb0b56ef818f95c3e58'
    })}\r\n`;

    fs.appendFileSync(program.file, entry);

    return cb();
  });
}

const q = async.queue(processKey, program.concurrency);
q.drain = () => {
  if (!receivedAll) {
    return;
  }

  console.log('[INFO]:', `finished export of ${count} keys to ${program.file}`);
};

function importToBucket() {
  if (!fs.existsSync(program.file)) {
    throw new Error('the import file does not exist');
  }

  fs.readFile(program.file, 'utf8', (err, data) => {
    if (err) {
      console.log('[ERROR]:', err);
      return;
    }
    const entries = data.split('\r\n');

    async.eachLimit(entries, program.concurrency, (entry, cb) => {
      try {
        if (!entry?.trim()) {
          cb();
          return;
        }

        entry = JSON.parse(entry);

        if (entry.props) {
          entry.data = JSON.stringify({props: entry.props});
          entry.headers = {
            'content-type': 'application/json',
          };
          entry.url = createKey('props');
        } else {
          if (!entry.data) {
            console.warn('[WARN]:', `Key: ${entry.key} is empty`);
            cb();
            return;
          }

          if (!['Object', 'number'].includes(typeof entry.data)) {
            entry.data = new Buffer(entry.data, 'base64');
            entry.headers['content-encoding'] = entry.headers['content-encoding'] || 'gzip';
          } else {
            entry.data = JSON.stringify(entry.data);
          }

          entry.url = createKey(entry.key);
        }
      } catch (err) {
        cb(err);
      }

      console.log('[INFO]:', `Inserting ${entry.url}`);

      request(entry.url, {
        method: 'PUT',
        headers: entry.headers,
        body: entry.data,
      }, (err, response) => {
        if (err || response?.statusCode !== 204) {
          cb(err || JSON.stringify(response));
          return;
        }
        cb();
      });
    }, (err) => {
      if (err) {
        console.log('[ERROR]:', err);
        return;
      }
      console.log('[INFO]:', `${entries.length} entries inserted into bucket ${bucket}`);
    });
  });
}

function exportFromBucket() {
  if (fs.existsSync(program.file)) {
    throw new Error('the output file already exists');
  }
  const bucketKeysUrl = `${[riakUrl, bucket].join('/')}?keys=stream`;
  console.log('[INFO]:', `fetching keys: ${bucketKeysUrl}`);

  let buffer = '';

  request(bucketKeysUrl, (err) => {
    if (err) {
      console.log('[ERROR]:', `failed to fetch keys: ${err}`);
    }
  }).on('data', (data) => {
    let validJson = true;
    const dataArr = `${buffer}${data}`.replaceAll('}{', '}~~{').split('~~');
    const parsedData = dataArr.reduce((acc, cur) => {
      const chunk = isValidJSON(cur);
      if (!chunk) {
        console.log('[WARN]:', `Broken chunk: ${cur}`);
        validJson = false;
      } else {
        acc = mergeWith(acc, chunk, (dst, src) => {
          if (dst instanceof Array) {
            return dst.concat(src);
          }
        });
      }
      return acc;
    }, {});

    if (!validJson) {
      buffer += data;
      return;
    }
    buffer = '';

    if (parsedData.props) {
      fs.appendFileSync(program.file, `${JSON.stringify(parsedData)}\r\n`);
    } else if (parsedData.keys) {
      count += parsedData.keys.length;
      parsedData.keys.forEach(k => {
        openWrites++;
        q.push(k, () => openWrites--);
      });
    } else {
      console.log('[ERROR]:', `Unreachable code: ${JSON.stringify(parsedData)}`);
    }
  }).on('end', () => {
    console.log('[INFO]:', 'received all keys');
    receivedAll = true;
  });
}

// --
if (program.import) {
  importToBucket();
} else {
  exportFromBucket();
}
