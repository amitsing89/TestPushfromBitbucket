var randToken = require('rand-token');
var crypto = require('crypto');

var tokenConf = require('../config/tokenConf.js');
var logger = require('../config/logger.js');
var rules = require('./rules.js');

var symbolsArr = ['+', '-', '.', '@', '?', '$', '%', '/', '*', ' ','_']; //keep enhancing this based on requirements

var utilHandler = {
    //method to return hidden chars for a given length
    createHiddenChars: function (size, character) {
        var hideCharToAppend = '';
        var characterToMask = character == 'default' ? tokenConf.maskChar : character;
        for (var i = 0; i < size; i++) {
            hideCharToAppend = hideCharToAppend + characterToMask;
        }

        return hideCharToAppend;
    },
    //method to release the connection
    doRelease: function (connection) {
        connection.close(
            function (err) {
                if (err) { console.error(err.message); }
                else { console.log("connection released"); }
            });
    },

    //method to create token based on regex
    createToken: function () {
        if (arguments.length > 0) {
            var inputText = arguments[0] ? arguments[0] : "";
            var flag = arguments[1] ? arguments[1] : true;
            var inputStr = inputText.toString();
            //if saveFormat flag is off then blindly give the token tested against any regex
            if (!flag) {
                return testRegexAndGenerateToken(inputStr);
            } else {
                //preserve the symbols
                var range = arguments[3] ? arguments[3] : null;
                var formattedOutput = splitAtSymbols(inputStr,range); //get output excluding the symbols
                return formattedOutput;
            }
        }
    }
}

function splitAtSymbols() {

    if (arguments.length > 0) {
        var input = arguments[0];
        var range = arguments[1];
    }

    var part = "";
    var token = "";
    for (var i = 0; i < input.length; i++) {
        if (symbolsArr.indexOf(input[i]) != -1) {
            //found a symbol, call testRegexAndGenerateToken for part
            symbol = input[i];
            partToken = testRegexAndGenerateToken(part);
            token = token.concat(partToken).concat(symbol);
            part = "";
        } else {
            part = part + input[i];
        }
    }
    //not leaving the end part
    partToken = testRegexAndGenerateToken(part);
    token = token.concat(partToken);
    return token;
}

function testRegexAndGenerateToken() {

    if (arguments.length > 0) {

        var field = arguments[0];
        var range = arguments[1];

        var numberRegex = new RegExp(/^\d+$/); //match numbers
        var alphabetRegex = new RegExp(/^[a-zA-Z]+$/); //match alphabets
        var size = field.length;

        if (numberRegex.test(field)) {
            //logger.info('info', 'using ' + rules.NUMBERULE + 'rules to anonymise the data set');
            chars = '0-9';
            if (range) {
                console.log('range connection', range);
                //only when it is a number use Math.random since the range paramter is restricting us from giving less randomness
                return getRandomInt(range.lRange, range.uRange);
            }
        } else if (alphabetRegex.test(field)) {
            //logger.info('info', 'using ' + rules.ALPHABETRULE + 'rules to anonymise the data set');
            //check for case
            chars = isUpperCase(field) ? 'A-Z' : 'a-z'
        } else {
            //logger.info('info', 'using ' + rules.ALPHANUMERICRULE + 'rules to anonymise the data set');
            //alphanumeric
            chars = 'default';
        }
        var customGenerator = tokenGenerator(chars);
        return customGenerator.generate(size);
    }

}

//private method to generate the token constructor
function tokenGenerator(type) {
    var generator = randToken.generator({
        chars: type,
        source: crypto.randomBytes
    });
    return generator;
}

function isUpperCase(str) {
    return str === str.toUpperCase();
}

function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    console.log("in getRandomInt", max, min);
    return Math.floor(Math.random() * (max - min)) + min;
}

module.exports = utilHandler;