const Readable = require('stream').Readable;
const Riak = require('basho-riak-client');
const LimitedParallelStream = require('./limited_parallel_stream').LimitedParallelStream;


class ListKeys extends Readable {
    constructor(options = {}){
        options.objectMode = true;
        super(options);

        this.riak = options.riak;
        this.bucket = options.bucket;
    }

    _read(){
        this.riak.listKeys({
            stream: true,
            bucket: this.bucket
        }, (err, data) =>{
            if(err) {
                return this.emit('error', err);
            }

            if(data.keys) {
                this.push(JSON.stringify(data.keys))
            }

            if(data.done) {
                return this.push(null)
            }

        })
    }
}

class Pb {

    constructor(options){
        this.bucket = options.bucket;
        const node = new Riak.Node({
            remoteAddress: options.host,
            remotePort: options.port
        });
        const cluster = new Riak.Cluster.Builder().withRiakNodes([node]).build();
        this.riak = new Riak.Client(cluster, (err) =>{
            if(err) {
                return console.error(err)
            }

            console.info('\t Riak connected ✔');
        })
    }

    listKeys(){
        return new ListKeys({
            riak: this.riak,
            bucket: this.bucket
        })
    }

    fetchObject(key, concurrency){
        const bucket = this.bucket;
        const riak = this.riak;

        return new LimitedParallelStream(concurrency, function(key, enc, done) =>{
            riak.fetchValue({
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
        })

    }
}

module.exports.create = (options) =>{
    return new Pb(options);
};