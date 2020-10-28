package com.bt.taas.flume.interceptor;


import com.bt.taas.http.processor.HttpProcessor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;


import java.io.IOException;
import java.util.List;

/**
 * Created by 611432986 on 04/04/2017.
 */

public class FlumeInterceptor implements Interceptor {

    public static String accessToken = null;
    public static HttpProcessor httpProcessor = new HttpProcessor();

    static {

        if (null == accessToken) {
            String filepath = "configuration.properties";
            //invoke login request
            httpProcessor.setProperties(filepath);

            try {
                accessToken = httpProcessor.getAuthKey();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //based on header will decide if its is tokenize request or detokenize

        String input = new String(event.getBody());
        String[] inputFields = input.split(",");
        StringBuilder output = new StringBuilder();
        for (String field : inputFields) {
            String tokenField = httpProcessor.getTokenizeValue(accessToken, field, "0", "0");
            output.append(tokenField + ",");
        }
        event.setBody(new String(output).getBytes());
        return event;
    }


    @Override
    public List<Event> intercept(List<Event> list) {
        return list;
    }

    @Override
    public void close() {

    }


    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new FlumeInterceptor();
        }

        @Override
        public void configure(Context context) {}
    }
}