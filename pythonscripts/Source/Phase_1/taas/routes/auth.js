var jwt = require('jwt-simple');
var Keytool = require('node-keytool');
var fs = require('fs');
var expiry = require('../config/expiry.js');

//destination folder
var uploadFolder = 'uploads\\';

var auth = {
    //authorize the user credentials to login
    login: function (req, res) {
        var username = req.body.username || '';
        //password is read from .jks file
        if (!req.file) {
            return res.json({
                "status": 401,
                "message": "Please specify the keystore password file"
            });
        }
        var filepath = req.file.path || '';
        var aliasName = '';

        //read data
        fs.readFile(filepath, function (err, data) {
            if (err) throw err;
            //write data to a jks file and create keystore instance to verify the password
            fs.writeFile(uploadFolder + req.file.originalname, data, function (err) {
                if (err) throw err;
                console.log("succesfully written");

            });
            //delete the previous uploaded file
            fs.unlink(req.file.path, function (err) {
                if (err) throw err;
                console.log('successfully deleted ' + req.file.path);
            });

        });
        //create keytool instance
        var store = Keytool(uploadFolder + req.file.originalname, 'password', { debug: false, storetype: 'JKS' });

        store.list(function (err, ksres) {
            //console.log(ksres);
            var response = res;
            ksres.certs.forEach(function (element) {
                aliasName = element.alias;
                //console.log(element.alias, aliasName);
                var dbUser = auth.validate(username, aliasName);
                if (!dbUser) { // if authentication fails send him 401
                    response.status(401);
                    response.json({
                        "status": 401,
                        "message": "Invalid credentials"
                    });
                    return;

                }

                if (dbUser) {
                    //authentication successful; generate a token
                    response.json(genWebToken(dbUser));
                }

            });
        });

        if (username == '') {
            res.status(401);
            res.json({
                "status": 401,
                "message": "Invalid credentials"
            });
            return;

        }


    },

    validate: function (username, aliasName) {

        if (username == aliasName) {
            var dbUserObj = {
                'name': username,
                'role': aliasName

            };
        }
        return dbUserObj;

    }

}

// private method
function genWebToken(user) {
    var expires = expiresIn(expiry.expiryPeriod);
    var token = jwt.encode({
        iss: user.name,
        exp: expires
    }, require('../config/secret.js')());
    return {
        token: token
    };
}

function expiresIn(numDays) {
    var dateObj = new Date();
    return dateObj.setDate(dateObj.getDate() + numDays);
}

module.exports = auth;