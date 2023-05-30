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
    -i, --import           import mode (instead of reading from bucket entries will be written to bucket)
    -c, --concurrency      specify the concurrency (default: 20)
    -O, --only-keys        export Only keys without data
    -f, --file [FileName]  specify the file name (default: [bucket[_keys]].json)
````
