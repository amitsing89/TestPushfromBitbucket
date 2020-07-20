var express = require('express');
var router = express.Router();
var auth = require('./auth');
var validateReq = require('../middleware/validateRequests');
var tokenHandler = require('./oraTokenRulesHandler.js');
var policyHandler = require('./policyHandler.js');
var multer = require('multer');
var upload = multer({ dest: 'uploads/' })

/* GET home page. */
router.get('/', function (req, res, next) {
  /* create db pool */
   res.sendfile('index.html'); 

});

/* first time login to generate a token */
router.post('/login', upload.single('password'), auth.login);

/* to get token for the user input */
router.post('/api/getToken', [validateReq.validateToken], tokenHandler.setToken);

/* to get input back from the token*/
router.post('/api/getText', [validateReq.validateToken], tokenHandler.getInput);

/* updatePolicy request will create a policy if it is a new request else modify the same with any changes. */
router.post('/api/updatePolicy', [validateReq.validateToken], policyHandler.updatePolicy);

/* only for security officer */
router.delete('/api/deletePolicy/:policyKey', [validateReq.validateToken], policyHandler.deletePolicy);

/** get policy details based on the field */
router.get('/api/getPolicy/:policyKey/:field', [validateReq.validateToken], policyHandler.getPolicy);

/** to get the complete policy data */
router.get('/api/getPolicy/:policyKey/', [validateReq.validateToken], policyHandler.getPolicy);

/**get the policy keys */
router.get('/api/getPolicyKeys',[validateReq.validateToken],policyHandler.getPolicyKeys);

/**  get last 10 policies - params , date/volume*/
router.get('/api/getAllPolicies',[validateReq.validateToken], policyHandler.getAllPolicies);
module.exports = router;

