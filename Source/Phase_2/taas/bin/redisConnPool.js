var RedisPool = require('redis-connection-pool');
var db = require('../config/db.js');

var redisPool;

var redisConnPool = {

    handleRedisPool: function (req, res, callback) {
        if (!redisPool) {
            console.log("connecting to redis , creating a pool of connections");
            redisPool = require('redis-connection-pool')('myRedisPool', {
                host: db.redisHost, // default
                port: db.redisPort, //default
                // optionally specify full redis url, overrides host + port properties
                // url: "redis://username:password@host:port"
                max_clients: 30, // defalut
                perform_checks: false, // checks for needed push/pop functionality
                database: 0, // database number to use
                options: {
                    //auth_pass: 'password'
                } //options for createClient of node-redis, optional
            });
            console.log("successfully connected to redis db");
        }
        callback(req, res, redisPool);

    }
}

module.exports = redisConnPool;