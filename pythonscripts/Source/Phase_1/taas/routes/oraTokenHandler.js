var crypto = require('crypto');
var randToken = require('rand-token');

var dbPoolHandler = require('../bin/dbPool.js');
var db = require('../config/db.js');

//create connection pool using the db.js
var oraTokenHandler = {

    setToken: function (req, res) {
        var input = req.body.input;
        //error handling
        if (!input) {
            return res.status(400).json({
                message: 'Request is not properly formed, Please specify the input parameter',
            });
        }

        var plainText = typeof input == 'string' ? input : input.toString();
        var leading = false;
        var trailing = false;

        var prefix = req.body.prefix;
        var postfix = req.body.postfix;

        // in certain unsual cases when prefix and postfix is equal or greater than the length of input , ignore such request
        if (prefix && postfix) {
            if (prefix + postfix >= plainText.length) {
                return res.status(400).json({
                    message: 'Please check the prefix and postfix value.'
                })
            }
        }

        var trimmedInput = prefix && postfix ? plainText.substring(prefix, plainText.length - postfix) : prefix ? plainText.substring(prefix) : postfix ? plainText.substring(0, plainText.length - postfix) : plainText;
        var leading = prefix ? true : false;
        var trailing = postfix ? true : false;

        var leadingChars = leading ? plainText.substring(0, prefix) : '';
        var trailingChars = trailing ? plainText.substring(plainText.length - postfix) : '';

        //ignore all symbols
        var trimmedInput = trimmedInput.replace(/([-.*+?^=!:&$])/g, '');

        dbPoolHandler.handleConnectionPool(req, res, function (request, response, connection) {
            //check if input already exists in db , fetch the associated token  else set new
            var selectQuery = "SELECT " + db.columns[0] + "," + db.columns[1] + " FROM " + db.table + " where " + db.columns[0] + "= :plainText";

            connection.execute(
                selectQuery,
                [plainText], //bind params
                function (err, result) {
                    if (err) {
                        doRelease(connection);
                        return response.status(500).json({
                            message: "Error occured while fetching the plain text",
                            error: err
                        });
                    }

                    //if input already exists do not create new 
                    if (result.rows.length > 0) {
                        tokenOutput = result.rows[0].TOKEN_VALUE;
                        doRelease(connection);
                        return response.status(200).json({
                            message: 'Input already exists',
                            obj: result.rows[0]
                        });
                    }
                    else {
                        //get token based on trimmedInput                     
                        tokenOutput = createTokenOptions(trimmedInput);
                        finalOutput = leading && trailing ? leadingChars + tokenOutput + trailingChars : leading ? leadingChars + tokenOutput : trailing ? tokenOutput + trailingChars : tokenOutput;

                        var insertQuery = "Insert into " + db.table + " Values (:plainText, :tokenValue)";

                        connection.execute(insertQuery,
                            [plainText, finalOutput],
                            { autoCommit: true },
                            function (err, result) {
                                if (err) {
                                    doRelease(connection);
                                    return response.status(400).json({
                                        message: err.message.indexOf("ORA-00001") > -1 ? "input already exists" : "error while inserting",
                                        error: err
                                    });
                                }

                                // Get amount of inserted rows.
                                // This will be 1 but gives a nice example of how to use rowsAffected
                                console.log(result.rowsAffected + " row inserted.");

                                var doc = { PLAIN_TEXT: plainText, TOKEN_VALUE: finalOutput };
                                response.status(200).json({
                                    message: 'Saved token',
                                    obj: doc
                                });
                                doRelease(connection);
                                return;
                            }
                        ); //end of insert query
                    }

                });  //end of  select query


        });

    },

    getInput: function (req, res) {

        var tokenValue = req.body.input;

        //get plain input for the requested token
        dbPoolHandler.handleConnectionPool(req, res, function (request, response, connection) {
            var selectQuery = "Select " + db.columns[0] + ", " + db.columns[1] + " from " + db.table + " where " + db.columns[1] + " =:tokenValue";
            connection.execute(selectQuery,
                [tokenValue],
                function (err, result) {
                    if (err || result.rows.length < 1) {
                        doRelease(connection);
                        var status = err ? 500 : 400;
                        return res.status(status).json({
                            message: err ? "an error occured in getting the token" : "no such entry exists, please tokenize the input first",
                            error: err
                        });
                    }
                    doRelease(connection);
                    return res.status(200).json({
                        "message": "found token, returning the plain text",
                        "obj": result.rows[0]

                    });


                }
            ) //end of select query

        });


    }

}

//private method
function doRelease(connection) {
    connection.close(
        function (err) {
            if (err) { console.error(err.message); }
            else { console.log("connection released"); }
        });
}


//private method
function tokenGenerator(type) {
    var generator = randToken.generator({
        chars: type,
        source: crypto.randomBytes
    });

    return generator;

}

//private method
function createTokenOptions(inputText) {

    console.log("inside createTokenOptions", inputText);
    //check for regex 
    var numberRegex = new RegExp(/^\d+$/); //match numbers
    var alphabetRegex = new RegExp(/^[a-zA-Z]+$/); //match alphabets

    //default options
    var size = inputText.length;
    var chars = 'default';

    if (numberRegex.test(inputText)) {
        size = inputText.toString().length;
        chars = '0-9';
    }

    if (alphabetRegex.test(inputText)) {
        //check for case
        chars = isUpperCase(inputText) ? 'A-Z' : 'a-z'
    }


    var customGenerator = tokenGenerator(chars);
    var tokenOutput = customGenerator.generate(size);

    console.log("inside createTokenOptions =>", tokenOutput);

    return tokenOutput;
}

function isUpperCase(str) {
    return str === str.toUpperCase();
}

module.exports = oraTokenHandler;