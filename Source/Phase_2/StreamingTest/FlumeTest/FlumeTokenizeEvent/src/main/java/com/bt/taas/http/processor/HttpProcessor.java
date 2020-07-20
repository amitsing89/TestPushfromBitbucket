package com.bt.taas.http.processor;

import static com.bt.taas.util.Constant.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.DefaultHttpClient;


public class HttpProcessor {

    //Tokenization tokenization = new Tokenization();
    HttpProperties tokenization = new HttpProperties();

    /**
     * This method load the config file and set all the properties
     *
     * @param configfilepath
     */
    public void setProperties(String configfilepath) {
        Properties prop = new Properties();
        InputStream input = null;

        String url = "";
        String port = "";
        String jksfilepath = "";
        String username = "";
        try {
            input = new FileInputStream(configfilepath);
            prop.load(input);
            url = prop.getProperty("URL");
            port = prop.getProperty("Port");
            jksfilepath = prop.getProperty("jksfilepath");
            username = prop.getProperty("username");
            //System.out.println("Url " +url +" port "+port+" jksfilepath "+jksfilepath +"username");
            if ("".equalsIgnoreCase(url)) {
                throw new Error("Url cannot be blank");
            } else
                tokenization.setHost(url);
            if ("".equalsIgnoreCase(port)) {
                throw new Error("Port cannot be blank");
            } else
                tokenization.setPort(port);

			if ("".equalsIgnoreCase(jksfilepath)) {
				throw new Error("keystore filepath cannot be blank");
			} else
				tokenization.setJksfilePath(jksfilepath);
            if ("".equalsIgnoreCase(username)) {
                throw new Error("username cannot be blank");
            } else
                tokenization.setUsername(username);


        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * This Method will call login services using username and password file.
     *
     * @return autkey
     * @throws ClientProtocolException
     * @throws IOException
     */
    public String getAuthKey() throws ClientProtocolException, IOException {
        //String service = "login";
        String url = HTTP + tokenization.getHost() + ":" + tokenization.getPort() + "/" + LOGIN_SERVICE;
        //System.out.println("url "+url);
        HttpEntity entity = MultipartEntityBuilder.create().addTextBody("username", tokenization.getUsername()).addBinaryBody("password",
                new File(tokenization.getJksfilePath()), ContentType.create("application/octet-stream"), tokenization.getJksfilePath()).build();

        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(url);

        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        //System.out.println("Response Code : " + response.getStatusLine().getStatusCode());
        if (response.getStatusLine().getStatusCode() != SUCCESS_CODE) {
            throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());

        }

        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

        // StringBuffer result = new StringBuffer();
        String line = "";
        String authkey = "";
        while ((line = rd.readLine()) != null) {
            // result.append(line);
            if (line.contains("token")) {
                authkey = line.split(":")[1].replaceAll("\"", "").replace("}", "");
            }
        }

        return authkey;

    }


    /**
     * This method accepts authtoken and number which needs to be tokenized
     *
     * @param authtoken authorized token which got generated using loging services
     * @param number    number which needs to be tokenized
     * @return tokenized value
     */
    public String getTokenizeValue(String authtoken, String number, String prefix, String postfix) {
        // String authtoken1="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhZG1pbiIsImV4cCI6MTQ4NDIwNDA1NDE1MX0.s1Ql1zas-4d9YVMxFGuak3iRGajmFuplWZ9ZQ-TZobg";
        String token_value = "";
        BufferedReader br;
        try {

            //String service = "api/getToken";
            String input = "";
            String urlstring = HTTP + tokenization.getHost() + ":" + tokenization.getPort() + "/" + GET_TOKEN;
            URL url = new URL(urlstring);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod(REQUEST_TYPE);
            conn.setRequestProperty(CONTENT_TYPE, "application/json");
            conn.setRequestProperty(ACCESS_TOKEN, authtoken);
            if (("0".equalsIgnoreCase(prefix) && ("0".equalsIgnoreCase(postfix)))) {
                input = "{\"input\":" + '"' + number + '"' + "}";
            }
            if (!("0".equalsIgnoreCase(prefix)) && ("0".equalsIgnoreCase(postfix))) {
                input = "{\"input\":" + '"' + number + '"' + ",\"prefix\":" + prefix + "}";
            }
            if (("0".equalsIgnoreCase(prefix)) && !("0".equalsIgnoreCase(postfix))) {
                input = "{\"input\":" + '"' + number + '"' + ",\"postfix\":" + postfix + "}";
            }
            if (!("0".equalsIgnoreCase(prefix)) && !("0".equalsIgnoreCase(postfix))) {
                input = "{\"input\":" + '"' + number + '"' + ",\"prefix\":" + prefix + ",\"postfix\":" + postfix + "}";
            }
            OutputStream os = conn.getOutputStream();
            os.write(input.getBytes());
            os.flush();

            if (conn.getResponseCode() != SUCCESS_CODE) {
                //System.out.println("inside error "+conn.getResponseCode());
                String message = "";
                String msg = "";
                br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
                while ((message = br.readLine()) != null) {
                    //System.out.println(" Buffer reader value inside error "+message);
                    if (message.contains("message")) {
                        msg = message.split(":")[1].replaceAll("\"", "").replaceAll("}", "");
                    }
                }
                throw new RuntimeException("Failed : HTTP error message : " + msg);

            }

            br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

            //System.out.println("Output from Server .... \n");

            String output = "";
            while ((output = br.readLine()) != null) {
                //System.out.println(" Buffer reader value "+output);
                if (output.contains("TOKEN_VALUE")) {
                    token_value = output.split(":")[4].replaceAll("\"", "").replaceAll("}", "");
                }
            }
            conn.disconnect();
            // return output;
        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }
        //System.out.println("Token value "+token_value);
        return token_value;

    }

    public String getOriginalValue(String authtoken, String number) {
        // String authtoken1="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhZG1pbiIsImV4cCI6MTQ4NDIwNDA1NDE1MX0.s1Ql1zas-4d9YVMxFGuak3iRGajmFuplWZ9ZQ-TZobg";
        BufferedReader br;
        System.out.println("authtoken " + authtoken);
        System.out.println("number " + number);
        String token_value = "";
        try {

            //String service = "api/getToken";
            String input = "";
            String urlstring = HTTP + tokenization.getHost() + ":" + tokenization.getPort() + "/" + GET_TEXT;
            URL url = new URL(urlstring);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod(REQUEST_TYPE);
            conn.setRequestProperty(CONTENT_TYPE, "application/json");
            conn.setRequestProperty(ACCESS_TOKEN, authtoken);
            input = "{\"input\":" + '"' + number + '"' + "}";
            OutputStream os = conn.getOutputStream();
            os.write(input.getBytes());
            os.flush();

            if (conn.getResponseCode() != SUCCESS_CODE) {
                System.out.println("inside error " + conn.getResponseCode());
                String message = "";
                String msg = "";
                br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
                while ((message = br.readLine()) != null) {
                    //System.out.println(" Buffer reader value inside error "+message);
                    if (message.contains("message")) {
                        msg = message.split(":")[1].replaceAll("\"", "").replaceAll("}", "");
                    }
                }

                throw new RuntimeException("Failed : HTTP error Message : " + msg);

            }

            br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

            System.out.println("Output from Server .... \n");

            String output = "";
            while ((output = br.readLine()) != null) {
                System.out.println(" Buffer reader value " + output);
                if (output.contains("PLAIN_TEXT")) {
                    token_value = output.split(":")[3].replaceAll("\"", "").replaceAll("}", "").split(",")[0];
                }
            }
            conn.disconnect();
            // return output;
        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }
        //System.out.println("original value "+token_value);
        return token_value;

    }

}
