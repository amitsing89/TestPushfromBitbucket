var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var schema = new Schema({
    plain_text: { type: String, required: true },
    token_value: { type: String, required: true },

})

module.exports = mongoose.model("Tokens", schema);