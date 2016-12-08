# simple-riak-migrator

Small command line utility to export bucket contents to a file and import from file

### How to use ###

* `npm install simple-riak-migrator`
* run `node index.js [bucketName]`
* `export will be in bucketName.json`

### Any options? ###
````
Usage: node index.js [options] bucketName

  Options:

    -h, --help             output usage information
    -V, --version          output the version number
    -H, --host [host]      specify the host (default: localhost)
    -p, --port [port]      specify the post (default: 8098)
    -f, --file [FileName]  specify the file name (default: [bucket].json)
    -i, --import           import mode (instead of reading from bucket entries will be written to bucket)
    -q, --queue            specify the concurrency (default: 20)
    -m, --meta             save meta if necessary (default: false)
    -P, --pretty           stringify with tabs (default: false)
    --delete               delete the keys as they are exported (DANGER: possible data loss)
````
