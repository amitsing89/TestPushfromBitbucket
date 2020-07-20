package com.bt.taas.http.processor;

public class HttpProperties {
	String port="";
	String jksfilePath="";
	String host="";
	String username="";
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}

	//file for taas.jks
	public String getJksfilePath() {
		return jksfilePath;
	}
	public void setJksfilePath(String jksfilePath) {
		this.jksfilePath = jksfilePath;
	}

}
