const stream = require('stream');
const util = require('util');

class LimitedParallelStream extends stream.Transform {
    constructor(concurrency, userTransform){
        super({objectMode: true});
        this.userTransform = userTransform;
        this.running = 0;
        this.terminateCallback = null;
        this.continueCallback = null;
        this.concurrency = concurrency;
    }

    _transform(chunk, enc, done){
        this.running++;
        this.userTransform(chunk, enc, this._onComplete.bind(this));
        if(this.running < this.concurrency) {
            done();
        }
        else {
            this.continueCallback = done;
        }
    }

    _onComplete(err, chunk){
        this.running--;
        if(err) {
            return this.emit('error', err);
        }
        const tmpCallback = this.continueCallback;
        this.continueCallback = null;
        tmpCallback && tmpCallback();
        if(this.running === 0) {
            this.terminateCallback && this.terminateCallback();
        }
    }

    _flush(done){
        if(this.running > 0) {
            this.terminateCallback = done;
        }
        else {
            done();
        }
    }

}

module.exports.LimitedParallelStream = LimitedParallelStream;
