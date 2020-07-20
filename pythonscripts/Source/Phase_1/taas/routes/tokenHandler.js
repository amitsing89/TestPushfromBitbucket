var crypto = require('crypto');
var randToken = require('rand-token');
var Tokens = require('../models/token.js');

var tokenHandler = {
    // request is validated and generate the token for the input
    generateTokenForInput: function (req, res) {
        var input = req.body.input;
        //error handling
        if (!input) {
            return res.status(400).json({
                title: 'Request is not properly formed, Please specify the input parameter',
            });
        }

        var plainText = typeof input == 'string' ? input : input.toString();
        var leading = false;
        var trailing = false;

        //if leading or trailing count exists check for the same
        if (req.body.prefix) {
            leading = true;
            var leadingCount = req.body.prefix;
            var trimmedInput = plainText.substring(leadingCount);
            var leadingChars = plainText.substring(0, leadingCount);

        }

        if (req.body.postfix) {
            trailing = true;
            var trailingCount = req.body.postfix;
            var trimmedInput = plainText.substring(0, plainText.length - trailingCount);
            var trailingChars = plainText.substring(plainText.length - trailingCount);
        }

        //check if input already exists in db , fetch the associated token else set new
        Tokens.findOne({ plain_text: plainText }, function (err, result) {
            if (err) {
                return res.status(500).json({
                    title: 'an error occured',
                    error: err
                });
            }
            else {
                input = trailing | leading ? trimmedInput : plainText;
                //console.log("final modified input", input, " with option leading", leading , " trailing", trailing);
                tokenOutput = result ? result.token_value : input ? createTokenOptions(input) : '';
                output = leading ? leadingChars + tokenOutput : trailing ? tokenOutput + trailingChars : tokenOutput;
                //console.log("after getting the token", output);

                //construct a token object and save to db
                var token = new Tokens({
                    plain_text: plainText,
                    token_value: output
                });
                token.save(function (err, result) {
                    if (err) {
                        return res.status(500).json({
                            title: 'an error occured',
                            error: err
                        });
                    }
                    else {
                        res.status(201).json({
                            message: 'Saved message',
                            obj: result
                        });
                        return;
                    }

                })
            }
        })

    },

    getInputFromToken: function (req, res) {
        //placeholder
        var tokenValue = req.body.input;
        console.log(tokenValue);

        //get plain input for the requested token
        Tokens.findOne({ token_value: tokenValue }, function (err, result) {
            if (err) {
                return res.status(500).json({
                    title: 'an error occured',
                    error: err
                });
            }
            else {
                if (!result) {
                    res.status(500).json({
                        message: 'no such entry exists, please tokenize the input first'
                    })
                }
                else {
                    res.status(200);
                    res.json({
                        "status": 200,
                        "message": "found token, returning the plain text",
                        "obj": result

                    });
                }

                return;
            }
        });

    }
}

//private method
function tokenGenerator(type) {
    console.log('type inside tokenGenerator', type);
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
    // regex for phone numbers with +91 or +91- followed by 10 digit
    var phoneNumberRegexOne = new RegExp(/\+\d+$/);
    var phoneNumberRegexTwo = new RegExp(/\+\d\d\-\d+$/);

    //default options
    var size = inputText.length;
    var chars = 'default';

    if (phoneNumberRegexOne.test(inputText)) {
        console.log("phoneNumberRegexOne");
        //ignore + symbol
        var modifiedInput = inputText.toString().substring(1);
        size = modifiedInput.length;
        chars = '0-9';
    }

    if (phoneNumberRegexTwo.test(inputText)) {
        console.log("phoneNumberRegexTwo");
        var inputString = inputText.toString();
        //ignore +  and -
        var modifiedString = inputString.substring(1, 2) + inputString.substring(3);
        size = modifiedString.length;
        chars = '0-9';
    }

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


module.exports = tokenHandler;