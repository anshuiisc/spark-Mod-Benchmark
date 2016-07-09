package py4j.examples;

import Topology.MsgIdAddandRemove;
import Topology.TetcCustomEventReceiver;
import logging.BatchedFileLogging;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.eclipse.paho.client.mqttv3.MqttClient;
import utils.GlobalConstants;

import java.io.IOException;
import java.io.Serializable;

public class StackEntryPoint implements Serializable {

    static MqttClient client=MQTTPublisher.connectToMqtt("tcp://localhost:61613", "inputstream" );

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        stream.writeObject(client);
    }
    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        client= (MqttClient) stream.readObject();
    }


    public JavaDStream<String> getdata()
 {
     String logFilePrefix1 = "IdentityTopology" + "-" + 111 + "-" + 0.01 + ".log1";
     String spoutLogFileName1 = "/Users/anshushukla/data/output/temp" + "/spout-" + logFilePrefix1;
     final BatchedFileLogging spoutlog1=new BatchedFileLogging(spoutLogFileName1, "spout");
     String datasetType = GlobalConstants.getDataSetTypeFromRunID("PLUG-11");
     String datafilename= "/Users/anshushukla/data/experi-smartplug-10min.csv"; //args[0];
     SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[4]");
     JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
     JavaDStream<String> sourcestream1 = ssc.receiverStream(
             new TetcCustomEventReceiver(datafilename,spoutlog1,1,datasetType));
     sourcestream1.print();
     ssc.start();
 return sourcestream1;
//     ssc.awaitTermination();
 }


    public static void main(final String[] args) {
        String logFilePrefix1 = "IdentityTopology" + "-" + 111 + "-" + 0.01 + ".log1";
        String spoutLogFileName1 = "/Users/anshushukla/data/output/temp" + "/spout-" + logFilePrefix1;
        final BatchedFileLogging spoutlog1=new BatchedFileLogging(spoutLogFileName1, "spout");
        String datasetType = GlobalConstants.getDataSetTypeFromRunID("PLUG-11");
//        String datafilename= "/Users/anshushukla/data/experi-smartplug-10min.csv"; //args[0];
        String datafilename="/Users/anshushukla/PycharmProjects/DataAnlytics1/py4jtest/daproject4secGap.csv";
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[4]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
        JavaDStream<String> sourcestream1 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog1,1,datasetType));


        sourcestream1.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
            @Override
            public Void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {

                stringJavaRDD.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
//                        System.out.println("inside here");
                        String s1=(System.currentTimeMillis()/1000)+",1,"+(Integer.parseInt(s.split("@")[1].split(",")[1]));
                        System.out.println("publish-"+s1);
                        MQTTPublisher.publishMessageNew(client,"inputstream",s1);
                    }
                });
                return null;
            }
        });




        //Use map the required data from each column

        JavaDStream<String> AirSpeed=sourcestream1.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
//                String AirSpeed=s.split("@")[1].split(",")[1];
                System.out.println(s);
                String AirSpeed=s.split(",")[0];
                System.out.println("AirSpeed-"+AirSpeed);
                return AirSpeed;
            }
        });



//reduce using 30 sec batch time

//        JavaDStream<String> AggAirSpeed=AirSpeed.reduce(new Function2<String, String, String>() {
//            @Override
//            public String call(String s, String s2) throws Exception {
//                long avg=(Long.parseLong(s) + Long.parseLong(s2));
//                System.out.println("Avg-"+avg);
//                return null;
//            }
//        });
        JavaDStream<String> AggAirSpeed = AirSpeed.reduceByWindow(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                long messageId1 = MsgIdAddandRemove.getMessageId(s);
                long messageId2 = MsgIdAddandRemove.getMessageId(s2);
                long messageId;
                messageId=(messageId1>messageId2)?messageId1:messageId2;
                String messageContent1 = MsgIdAddandRemove.getMessageContent(s);
                String messageContent2 = MsgIdAddandRemove.getMessageContent(s2);

                long avg=(Long.parseLong(messageContent1) + Long.parseLong(messageContent2))/2;
                System.out.println("Avg-"+avg);
                String s1=(System.currentTimeMillis()/1000)+",2,"+avg;
//                String s1=(100)+",2,"+15;
                System.out.println("publishNew-"+MsgIdAddandRemove.addMessageId(s1,messageId));
                MQTTPublisher.publishMessageNew(client,"inputstream",s1);
                return ""+avg;
            }
        }, Durations.seconds(7),Durations.seconds(7));


//

        JavaDStream<Double> AirDouble=AirSpeed.map(new Function<String, Double>() {
            @Override
            public Double call(String s) throws Exception {
                return Double.parseDouble(s);
            }
        });

        AirDouble.foreachRDD(new Function<JavaRDD<Double>, Void>() {
            @Override
            public Void call(JavaRDD<Double> doubleJavaRDD) throws Exception {
                Statistics.corr(doubleJavaRDD,doubleJavaRDD);

                return null;
            }
        });


//        trainingData = ssc.textFileStream("/training/data/dir").map(LabeledPoint.parse()).cache()
//        testData = ssc.textFileStream("/testing/data/dir").map(LabeledPoint.parse)



        AggAirSpeed.print();
        ssc.start();
//        MQTTPublisher.disConnectToMqtt(client);
//        getdata();
    }

}


//L   IdentityTopology   /Users/anshushukla/data/experi-smartplug-10min.csv   PLUG-1  0.01   /Users/anshushukla/data/output/temp