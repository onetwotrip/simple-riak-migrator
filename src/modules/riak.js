const adapters = {
    http: require('./http_adapter'),
    pb: require('./pb_adapter')
};

module.exports.create = (options)=>{
    return adapters[options.type || 'pb']
};