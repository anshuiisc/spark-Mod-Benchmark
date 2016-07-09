package py4j.examples;


import org.eclipse.paho.client.mqttv3.*;

import static java.lang.System.exit;

public class MQTTPublisher {

    public static void publishOutput(String Broker, String Topic, String Message) {
        MqttClient client;
        String clientID = "Spark";
        try {
            client = new MqttClient(Broker, clientID);

            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.isCleanSession();
            connOpts.setUserName("admin");
            connOpts.setPassword("password".toCharArray());
            client.connect(connOpts);
            MqttMessage message = new MqttMessage();
            message.setQos(0);

            // Sending output in small chunks
//            String msg;
//            for (int i = 1; i <= 10000; i++) {
//                msg = "Message " + i;
//                System.out.println(msg);
//                message.setPayload(msg.getBytes());
//                client.publish(Topic, message);
//            }

            message.setPayload(Message.getBytes());
            client.publish(Topic, message);
//            message.setPayload("ShutDown".getBytes());
//            client.publish(Topic, message);
            client.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }


    public static MqttClient connectToMqtt(String Broker, String Topic) {
        MqttClient client = null;
        String clientID = "Spark";
        try {
            client = new MqttClient(Broker, clientID);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.isCleanSession();
//            connOpts.setUserName("admin");
//            connOpts.setPassword("password".toCharArray());
            client.connect(connOpts);
        } catch (MqttSecurityException e) {
            e.printStackTrace();
        } catch (MqttException e) {
            e.printStackTrace();
        }
        return client;
    }

    public static void publishMessageNew(MqttClient client, String Topic,String Message) {
        MqttMessage message = new MqttMessage();
        message.setQos(0);
        message.setPayload(Message.getBytes());
        try {
            client.publish(Topic, message);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public static void disConnectToMqtt(MqttClient client) {
        try {
            client.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws MqttException {
        publishOutput("tcp://localhost:61613", "foo", "Message");
        exit(0);
    }
}
