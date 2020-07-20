var express = require('express');
var router = express.Router();
var auth = require('./auth');
var validateReq = require('../middleware/validateRequests');
var tokenHandler = require('./oraTokenHandler.js');
var multer = require('multer');
var upload = multer({ dest: 'uploads/' })

/* GET home page. */
router.get('/', function (req, res, next) {
  /* create db pool */
  res.render('index', { title: 'Express' });

});

/* first time login to generate a token */
router.post('/login', upload.single('password'), auth.login);

/* to get token for the user input */
router.post('/api/getToken', [validateReq], tokenHandler.setToken);

/* to get input back from the token*/
router.post('/api/getText', [validateReq], tokenHandler.getInput);

module.exports = router;

