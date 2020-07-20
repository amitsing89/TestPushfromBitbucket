//connect to redis database
var db = require('../config/db.js');
var redisConnPool = require('../bin/redisConnPool');
var redis = require('redis'),
client = redis.createClient('6379','192.168.109.129');

var policyHandler = {

    updatePolicy: function (req, res) {
        var policy = req.body.policy;
        var hashKey = policy["name"];

        if (!policy) {
            return res.status(400).json({
                "message": "Please check your request params."
            })
        }
        redisConnPool.handleRedisPool(req, res, function (request, response, redisConnection) {

            db.redisFields.forEach(function (field) {
            console.log("inside updatePolicy", field);
                if (field in policy) {
                    redisConnection.hset(hashKey, field, JSON.stringify(policy[field]), function (err, reply) {
                        if (err) {
                            return res.status(400).json({
                                message: "Error occured while saving the metadata template."
                            })
                        }
                    });
                }

            });

           //when no errors
           redisConnection.hgetall(hashKey, function (err, reply) {
               var parsedObject = {};
               for (key in reply) {
                   parsedObject[key] = JSON.parse(reply[key]);
               }

               return res.status(200).json({
                   message: "successfully saved the metadata to db.",
                   object: parsedObject
               });

           });


        });
    },
    deletePolicy: function (req, res) {
        var hashKey = req.params.policyKey;

        if (!hashKey) {
            return res.status(400).json({
                "message": "Please provide a policy to delete."
            })
        }

        redisConnPool.handleRedisPool(req, res, function (request, response, redisConnection) {
            redisConnection.del(hashKey, function (err, reply) {

                var message = reply == 1 ? "successfully deleted the policy" : "error in deleting the policy";
                var status = reply == 1 ? 200 : 400;

                return res.status(status).json({
                    "message": message
                });
            });

        });
    },

    getPolicyKeys: function(req,res) {
        console.log("inside getPolicyKeys");
        client.keys('*', function (err, keys) {
            var status = err ? 400 : 200;
            var message = err ? "Error in fetching policy keys.Please try again" : keys ? "successfully fetched the policy keys" : "Empty result set.No keys exist.";
            if(err){
                console.log('err',err);
                return res.status(status).json({
                    "message": message,
                    "output": JSON.parse(reply)

                });
                
            }

            for(var i = 0, len = keys.length; i < len; i++) {
                console.log("Keys from redis",keys[i]);
            }

            return res.status(200).json({
                message: "successfully fetched the keys.",
                keys: keys
            });
        });         
    },

    getPolicy: function (req, res) {
        console.log("inside getPolicy");
        var hashKey = req.params.policyKey;
        var field = req.params.field;

        if (!hashKey) {
            return res.status(400).json({
                "message": "Please provide a policy to get details like source, user id etc."
            })
        }

        //get policy metadata
        redisConnPool.handleRedisPool(req, res, function (request, response, redisConnection) {
            if (field) {
                redisConnection.hget(hashKey, field, function (err, reply) {

                    var status = err ? 400 : 200;
                    var message = err ? "Error in fetching policy.Please check the policy key or try again" : reply ? "successfully fetched the policy metadata" : "Empty result set.No such policy exists.";

                    return res.status(status).json({
                        "message": message,
                        "output": JSON.parse(reply)

                    });
                });
            } else {
                redisConnection.hgetall(hashKey, function (err, reply) {

                    var status = err ? 400 : 200;
                    var message = err ? "Error in fetching policy.Please check the policy key or try again" : reply ? "successfully fetched the policy metadata" : "Empty result set.No such policy exists.";

                    var parsedObject = {};
                    for (key in reply) {
                        parsedObject[key] = JSON.parse(reply[key]);
                    }

                    return res.status(200).json({
                        message: "successfully saved the metadata to db.",
                        object: parsedObject
                    });
                });

            }


        });
    },
    getAllPolicies: function (req, res) {
        console.log("inside getAllPolicies");

        var limit = req.params.limit || 10;

    }
}

function getPolicyData(conn, hashKey) {

    //when no errors
    conn.hgetall(hashKey, function (err, reply) {
        var parsedObject = {};
        for (key in reply) {
            parsedObject[key] = JSON.parse(reply[key]);
        }

        return parsedObject;

    });

}
module.exports = policyHandler;
