var dbPoolHandler = require('../bin/dbPool.js');
var db = require('../config/db.js');
var tokenConf = require('../config/tokenConf.js');

//for helper methods
var utilHandler = require('../util/customUtilRulesHandler.js');
var rules = require('../util/rules.js');
//get user information
var auth = require('./auth.js');
var tokenRequest = require('../middleware/validateRequests.js');


//create connection pool using the db.js
var oraTokenPolicyHandler = {

    setToken: function (req, res) {
        var input = req.body.input;
        //error handling
        if (!input) {
            return res.status(400).json({
                message: 'Request is not properly formed, Please specify the input parameter',
            });
        }
        var plainText = typeof input == 'string' ? input : input.toString();
        var ruleId = req.body.rule;
        if (ruleId) {
            var policy = rules[ruleId];
        }
        //check for prefix and postfix
        var prefix = req.body.prefix;
        var postfix = req.body.postfix;
        // in certain unusual cases when prefix and postfix is equal or greater than the length of input , ignore such request
        if (prefix && postfix) {
            if (prefix + postfix >= plainText.length) {
                return res.status(400).json({
                    message: 'Please check the prefix and postfix value.'
                })
            }
        }

        if (policy == "irreversible") {
            //var finalOutput = processInputByRules(policy, plainText, prefix, postfix, hideChar, range);
            var finalOutput = utilHandler.createToken(plainText);
            console.log('irreversible policy');
            var doc = {};
            doc[db.columns[0]] = plainText;
            doc[db.columns[1]] = finalOutput;

            res.status(200).json({
                message: 'token returned but not saved in the data vault since it is an irreversible action',
                obj: doc
            });
            return;
        }
        var forget = policy == 'forget' ? true : false;
        dbPoolHandler.handleConnectionPool(req, res, function (request, response, connection) {
            //check if input already exists in db , fetch the associated token  else set new
            var selectQuery = "SELECT " + db.columns[0] + "," + db.columns[1] + " FROM " + db.table + " where " + db.columns[0] + "= :plainText";
            connection.execute(
                selectQuery,
                [plainText], //bind params
                function (err, result) {
                    //audit
                    if (err) {
                        utilHandler.doRelease(connection);
                        return response.status(500).json({
                            message: "Error occured while fetching the plain text",
                            error: err
                        });
                    }
                    //if input already exists do not create new 
                    if (result.rows.length > 0) {
                        //identify an anonymised account which need to be deleted when reversible encryption is applied (e.g. in the case of the right to be forgotten). 
                        if (forget) {
                            var deleteQuery = "Delete from " + db.table + " where " + db.columns[0] + "= :plainText";
                            connection.execute(deleteQuery,
                                [plainText], //bind params
                                function (err, out) {
                                    if (err) {
                                        utilHandler.doRelease(connection);
                                        return response.status(500).json({
                                            message: "Error occured while deleting the anonymised record.",
                                            error: err
                                        });
                                    }
                                    utilHandler.doRelease(connection);
                                    return response.status(200).json({
                                        message: 'Successfully identified and deleted the record successfully.',
                                        obj: result.rows[0]
                                    });
                                });//end of delete query
                        } else {
                            console.log(result.rows[0]);
                            utilHandler.doRelease(connection);
                            return response.status(200).json({
                                message: 'Input already exists.',
                                obj: result.rows[0]
                            });
                        }

                    } else {
                        if (forget) {
                            utilHandler.doRelease(connection);
                            return response.status(200).json({
                                message: 'There are no records to be deleted, Please tokenize the data first',
                            });
                        }
                        var finalOutput = processInputByRules(policy, plainText, prefix, postfix);
                        var insertQuery = "Insert into " + db.table + " Values (:plainText, :tokenValue)";
                        connection.execute(insertQuery,
                            [plainText, finalOutput],
                            { autoCommit: true },
                            function (err, result) {
                                if (err) {
                                    utilHandler.doRelease(connection);
                                    return response.status(400).json({
                                        message: err.message.indexOf("ORA-00001") > -1 ? "input already exists" : "error while inserting",
                                        error: err
                                    });
                                }

                                // Get amount of inserted rows.
                                // This will be 1 but gives a nice example of how to use rowsAffected
                                console.log(result.rowsAffected + " row inserted.");
                                var doc = {};
                                doc[db.columns[0]] = plainText;
                                doc[db.columns[1]] = finalOutput;

                                response.status(200).json({
                                    message: 'Saved token',
                                    obj: doc
                                });
                                utilHandler.doRelease(connection);
                                return;
                            }
                        ); //end of insert query
                    }
                });  //end of  select query
        });
    },

    getInput: function (req, res) {
        //users other than admin cannot request for the original text given a token
        if (tokenRequest.getTokenIssuer() != 'admin') {
            return res.status(400).json({
                message: 'user role does not gives the privilege to access the sensitive data'
            });
        }
        var tokenValue = req.body.input;
        //get plain input for the requested token
        dbPoolHandler.handleConnectionPool(req, res, function (request, response, connection) {
            var selectQuery = "Select " + db.columns[0] + ", " + db.columns[1] + " from " + db.table + " where " + db.columns[1] + " =:tokenValue";
            connection.execute(selectQuery,
                [tokenValue],
                function (err, result) {
                    if (err || result.rows.length < 1) {
                        //audit
                        utilHandler.doRelease(connection);
                        var status = err ? 500 : 400;
                        return res.status(status).json({
                            message: err ? "an error occured in getting the token" : "no such entry exists, please tokenize the input first",
                            error: err
                        });
                    }
                    utilHandler.doRelease(connection);
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
function processInputByRules(policy, plainText, prefix, postfix) {
    //check for policy to decide on the rules and token generation methods
    var saveFlag = true //by default we preserve the data format unless a policy ignoreFormat is specified.
    if (policy == 'ignoreFormat') {
        saveFlag = false;
        var finalOutput = utilHandler.createToken(plainText, saveFlag);
    }
    //mention all other rules condition inside else 
    else {
        if (policy == "range") {
            var range = tokenConf.defaultRange;
        }

        var leading = prefix ? true : false;
        var trailing = postfix ? true : false;

        var trimmedInput = leading && trailing ? plainText.substring(prefix, plainText.length - postfix) : prefix ? plainText.substring(prefix) : postfix ? plainText.substring(0, plainText.length - postfix) : plainText;
        var leadingChars = leading ? plainText.substring(0, prefix) : '';
        var trailingChars = trailing ? plainText.substring(plainText.length - postfix) : '';

        var tokenOutput = utilHandler.createToken(trimmedInput, saveFlag, range);

        if (policy == 'mask') {
            var hideChar = tokenConf.defaultMaskChar;
            var trailHideChars = utilHandler.createHiddenChars(trailingChars.length, hideChar);
            var leadHideChars = utilHandler.createHiddenChars(leadingChars.length, hideChar);

            var finalOutput = leadHideChars + tokenOutput + trailHideChars; //when no leading and trailing , default will be applied
            console.log("masking will change finalOutput ==>", finalOutput);
        }
        else {
            var finalOutput = leading && trailing ? leadingChars + tokenOutput + trailingChars : leading ? leadingChars + tokenOutput : trailing ? tokenOutput + trailingChars : tokenOutput;
            console.log("finalOutput with no masking", finalOutput);
        }
        //add more rules
    }
    return finalOutput;
}

module.exports = oraTokenPolicyHandler;