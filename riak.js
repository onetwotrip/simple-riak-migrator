const Riak = require('basho-riak-client');

/**
 *  @TODO: Move this to pb_adapter
 */


module.exports.onReady = (host, port, cb) =>{
	const node = new Riak.Node({
		remoteAddress: host,
		remotePort: port
	});
	const cluster = new Riak.Cluster.Builder().withRiakNodes([node]).build();
	return new Riak.Client(cluster, (err, res) =>{
		console.info('\t Riak connected ✔');
		cb(err, res)
	})
};
