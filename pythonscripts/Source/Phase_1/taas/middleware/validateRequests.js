var jwt = require('jwt-simple');

module.exports = function (req, res, next) {
    var token = (req.body && req.body.access_token) || (req.query && req.query.access_token) || req.headers['x-access-token'];
    if (token) {
        try {
            var decoded = jwt.decode(token, require('../config/secret.js')());
            //console.log(decoded);
            if (decoded.exp <= Date.now()) {
                res.status(400);
                res.json({
                    "status": 400,
                    "message": "Access Token has expired"

                });
                return;
            }
            // handle token here
            console.log("valid token");
            next();

        } catch (err) {
            res.status(401);
            res.json({
                "status": 401,
                "message": "Invalid Token"

            });
            return;
        }
    } else {
        res.status(400);
        res.json({
            "status": 400,
            "message": "No Token available"

        });
        return;
    }
}