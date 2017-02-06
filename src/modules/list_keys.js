const Readable = require('stream').Readable;

/**
 * @TODO: Remove this file. See pb_adapter.js
 */

class GetKeys extends Readable {
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

module.exports.create = (riak, bucket) =>{
	return new GetKeys({
		riak: riak,
		bucket: bucket
	})
};