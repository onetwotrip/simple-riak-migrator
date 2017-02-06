const request = require('request');
const LimitedParallelStream = require('./limited_parallel_stream').LimitedParallelStream;


class Http {

    constructor(options){
        this.baseUrl = `${options.host}:${options.port}/riak/${options.bucket}`;
    }

    listKeys(){
        return request(this.baseUrl)
    }

    fetchObject(key, concurrency){
        new LimitedParallelStream(concurrency, function(key, enc, done){
            request(`${this.baseUrl}/${key}`, (err, data) =>{
                if(err) {
                    this.emit('error', err);
                    return done();
                }

                if(data.statusCode !== 200) {
                    this.emit('error', new Error(`Wrong statusCode: ${data.statusCode}`));
                    return done();
                }

                this.push(`${key}\t${data.body}\t${data.headers['content-type']}\n`);
                done();
            });
        });
    }
}

module.exports.create = (options) =>{
    return new Http(options);
};