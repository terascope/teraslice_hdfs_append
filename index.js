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
    var endpoint = opConfig.connection ? opConfig.connection : 'default';

    //client connection cannot be cached, an endpoint needs to be re-instantiated for a different namenode_host
    opConfig.connection_cache = false;

    var clientService = getClient(context, opConfig, 'hdfs_ha');
    var client = clientService.client

    function prepare_file(client, filename, chunks, logger) {
        // We need to make sure the file exists before we try to append to it.
        return client.getFileStatusAsync(filename)
            .catch(function(err) {
                // We'll get an error if the file doesn't exist so create it.
                return client.mkdirsAsync(path.dirname(filename))
                    .then(function(status) {
                        return client.createAsync(filename, '');
                    })
                    .catch(function(err) {
                        if (err.exception === "StandbyException") {
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
                    return client.appendAsync(filename, chunk)
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

        function sendFiles() {
            var stores = [];
            _.forOwn(map, function(chunks, key) {
                stores.push(prepare_file(client, key, chunks, logger));
            });

            // We can process all individual files in parallel.
            return Promise.all(stores)
                .catch(function(err) {
                    if (err.initialize) {
                        logger.warn(`hdfs namenode has changed, reinitializing client`);
                        var newClient = clientService.changeNameNode().client;
                        client = newClient;
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

    return context.foundation.getConnection(clientConfig);
}

function schema() {
    //most important schema configs are in the connection configuration
    return {};
}

module.exports = {
    newProcessor: newProcessor,
    schema: schema
};

