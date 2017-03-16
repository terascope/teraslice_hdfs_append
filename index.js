'use strict';

var _ = require('lodash');
var Promise = require('bluebird');
var path = require('path');

/*
 Configuration
 // These should probably be terafoundation level configs
 namenode host
 namenode port
 user

 Overall this should be a plugin of some sort
 */


function newProcessor(context, opConfig, jobConfig) {
    var logger = jobConfig.logger;
    var canMakeNewClient = opConfig.namenode_list.length >= 2
    var endpoint = opConfig.connection ? opConfig.connection : 'default';
    var nodeNameHost = context.sysconfig.terafoundation.connectors.hdfs[endpoint].namenode_host;

    //client connection cannot be cached, an endpoint needs to be re-instantiated for a different namenode_host
    opConfig.connection_cache = false;

    var client = getClient(context, opConfig, 'hdfs');
    var hdfs = Promise.promisifyAll(client);

    function prepare_file(hdfs, filename, chunks, logger) {
        // We need to make sure the file exists before we try to append to it.
        return hdfs.getFileStatusAsync(filename)
            .catch(function(err) {
                // We'll get an error if the file doesn't exist so create it.
                return hdfs.mkdirsAsync(path.dirname(filename))
                    .then(function(status) {
                        logger.warn("I should be creating a file")
                        return hdfs.createAsync(filename, '');
                    })
                    .catch(function(err) {
                        if (canMakeNewClient && err.exception === "StandbyException") {
                            return Promise.reject({initialize: true})
                        }
                        else {
                            var errMsg = err.stack;
                            return Promise.reject(`Error while attempting to create the file: ${filename} on hdfs, error: ${errMsg}`);
                        }
                    })
            })
            .return(chunks)
            // We need to serialize the storage of chunks so we run with concurrency 1
            .map(function(chunk) {
                if (chunk.length > 0) {
                    return hdfs.appendAsync(filename, chunk)
                }
            }, {concurrency: 1})
            .catch(function(err) {
                //for now we will throw if there is an async error
                if (err.initialize) {
                    return Promise.reject(err)
                }
                var errMsg = err.stack ? err.stack : err
                return Promise.reject(`Error sending data to file: ${filename}, error: ${errMsg}, data: ${JSON.stringify(chunks)}`)
            })
    }

    return function(data, logger) {
        var map = {};
        data.forEach(function(record) {
            if (!map.hasOwnProperty(record.filename)) map[record.filename] = [];

            map[record.filename].push(record.data)
        });

        function sendFiles(){
            var stores = [];
            _.forOwn(map, function(chunks, key) {
                stores.push(prepare_file(hdfs, key, chunks, logger));
            });

            // We can process all individual files in parallel.
            return Promise.all(stores)
                .catch(function(err) {
                    if (err.initialize) {
                        logger.warn(`hdfs namenode has changed, reinitializing client`);
                        var newClient = makeNewClient(context, opConfig, nodeNameHost, endpoint);
                        hdfs = newClient.hdfs;
                        nodeNameHost = newClient.nodeNameHost;
                        return sendFiles()
                    }

                    var errMsg = err.stack ? err.stack : err
                    logger.error(`Error while sending to hdfs, error: ${errMsg}`)
                    return Promise.reject(err)
                });
        }

        return sendFiles();
    }
}

function makeNewClient(context, opConfig, conn, endpoint) {
    var list = opConfig.namenode_list;
    //we want the next spot
    var index = list.indexOf(conn) + 1;
    //if empty start from the beginning of the
    var nodeNameHost= list[index] ? list[index] : list[0];

    //TODO need to review this, altering config so getClient will start with new namenode_host
    context.sysconfig.terafoundation.connectors.hdfs[endpoint].namenode_host = nodeNameHost
    return {
        nodeNameHost: nodeNameHost,
        hdfs: Promise.promisifyAll(getClient(context, opConfig, 'hdfs'))
    }
}

function getClient(context, config, type) {
    var clientConfig = {};
    clientConfig.type = type;

    if (config && config.hasOwnProperty('connection')) {
        clientConfig.endpoint = config.connection ? config.connection : 'default';
        clientConfig.cached = config.connection_cache !== undefined ? config.connection_cache : true;

    }
    else {
        clientConfig.endpoint = 'default';
        clientConfig.cached = true;
    }

    return context.foundation.getConnection(clientConfig).client;
}

function schema() {
    return {
        user: {
            doc: 'User to use when writing the files. Default: "hdfs"',
            default: 'hdfs',
            format: 'optional_String'
        },
        namenode_list: {
            doc: 'A list containing all namenode_hosts, this option is needed for high availability',
            default: []
        }
    };
}

module.exports = {
    newProcessor: newProcessor,
    schema: schema
};