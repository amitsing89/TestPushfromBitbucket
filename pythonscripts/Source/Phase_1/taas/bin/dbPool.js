var oracledb = require('oracledb');
var db = require('../config/db.js');
oracledb.outFormat = oracledb.OBJECT;

var connectionPool;

var dbPool = {

    handleConnectionPool: function (req, res, callback) {

        if (!connectionPool) {
            console.log("initiating connection pool");
            //create one and get connection
            oracledb.createPool({
                //externalAuth:true, // setting this so that db credentials are not hard coded in the application
                connectString: db.connectString,
                user: db.username,
                password: db.password
            }, function (err, pool) {

                if (err) {

                    return res.status(500).json({
                        message: "Something went wrong in creating connection pool. Please check your username and password!",
                        error: err
                    })
                }
                //get connection from pool
                connectionPool = pool; //store the pool instance to check if it is available for the next request
                //console.log(connectionPool);
                pool.getConnection(function (err, connection) {
                    if (err) {
                        return res.status(500).json({
                            message: "Something went wrong in connecting to the DB. Please check if your database service is running!",
                            error: err
                        })
                    }
                    console.log("successfully connected to the database");
                    callback(req, res, connection);

                });
            });
        }
        else {

            connectionPool.getConnection(function (err, connection) {
                if (err) {
                    return res.status(500).json({
                        message: "Something went wrong in connecting to the DB. Please check your username and password!",
                        error: err
                    })
                }
                console.log("successfully connected to the database");
                callback(req, res, connection);

            });

        }

    },

    closePool: function () {

        if (!connectionPool) {
            console.log("connection pool not initialized yet");
        }
        else {
            connectionPool.close(function (err) {
                if (err) { console.error(err.message); }
                else { console.log("application is terminating so connection pooling is closed"); }
            });
        }

    }

}

module.exports = dbPool;

