var dbConst = {
	//oracledb on UAT server
	//connectString: "cbl11276clu01-scan-oravip.dci.bt.com:61901/usrpp_taas",
	table: "data_vault",
	columns: ['RAW_DATA', 'TOKEN_DATA'],

	//oracledb on localhost if you are using oracle 10 XE
	 connectString: "localhost:1521/XE",
	 username: "TAAS",
	 password: "oracle",
	
	//mongo
	mongoUrl: "localhost:27017/vault",
	//redis url
	redisPort: "6379",
	redisHost: "192.168.142.135",
	redisFields: ["columns","metadata","status"],
	hdfsFields: ["keys","metadata","status"]

}

module.exports = dbConst;