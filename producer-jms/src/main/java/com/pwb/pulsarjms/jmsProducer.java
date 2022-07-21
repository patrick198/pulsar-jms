package com.pwb.pulsarjms;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.Message;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;

public class jmsProducer {
	private static final String SERVICE_URL = "pulsar+ssl://pulsar-gcp-uscentral1.streaming.datastax.com:6651";
	private static final String YOUR_PULSAR_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTc5MTUzMDQsImlzcyI6ImRhdGFzdGF4Iiwic3ViIjoiY2xpZW50OzRhYmY1MzA4LWE1MTUtNGZhOS1iYWVjLTk0ZDE5M2U2ZjA5YztjSGRpWlhoaGJYQnNaUT09O2Y5NDAyYTU5MDEiLCJ0b2tlbmlkIjoiZjk0MDJhNTkwMSJ9.XjS7g_kGcnEuRnK4IeRazfP64YJZCZlAadqmQf3j2GQt11fMwzkIYSe0LUPzhTbbRYEOGIEULKvekjBLgnn2Wnx0Lhw-DS5yvYwLnFlL5fH3XefiYOK9E3yxH157kM1defVo18hVeRvLR_347KuldyRcyrObmSxrEswbak6ZfyfT3LsNRw1jLISo8OA_fells6M9OkUwwUVOtNvOzsJauXoN2pNuIZ-5KHnJQrN7F2kN2YAg6xGRjbWHSGP7h1QBi5sbV6h4jAcAfGI97eHOGUsLtR48B-oaJ34pdtRhnLMlPiuZDnaGYiue187e9LCk092Q2MhdQYLiNvA95ouKfg";

	public static void main(String[] args) {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put("webServiceUrl", "https://pulsar-gcp-uscentral1.api.streaming.datastax.com");
        configuration.put("brokerServiceUrl", "pulsar+ssl://pulsar-gcp-uscentral1.streaming.datastax.com:6651");
        configuration.put("authPlugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        configuration.put("authParams", YOUR_PULSAR_TOKEN);
        PulsarConnectionFactory factory = new PulsarConnectionFactory(configuration);

        try (JMSContext context = factory.createContext()) {
//          Destination destination = context.createQueue("persistent://pwbexample/iot-sensor/sensor-temps");
            Destination destination = context.createQueue("persistent://pwbexample/iot-sensor/sensor-temps");
            Message msg = context.createTextMessage("Temp 90;Status=0;Battery=10");
            context.createProducer().send(destination, msg);
            System.out.printf("Producer message sent\n");
            context.close();
            System.exit(0);
          }
      catch (Exception e) {
				System.err.println("Error creating queue factory: ");
				e.printStackTrace();
				System.exit(1);
		}
    }


}

