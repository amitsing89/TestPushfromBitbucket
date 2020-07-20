var rules = {
    //default: 'reversible with data type, format and length is saved'
    //policies
    "1":'mask', //for this prefix and postfix is required to know how many characters to mask
    "2":'hash',
    "3":'range', // specify the range :ex: 100-200
    "4":'default', // by default we use tokenization algorithm and format is saved
    "5":'default', //same as tokenization
    "6":'redact',
    "7":'order',    //specify the order of 'ascending', 'descending' or any other format
    "8":'irreversible',
    "9":'ignoreFormat', //do not check for any prefix and postfix,
    "10":'forget'//only in reversible case
}   

module.exports = rules;