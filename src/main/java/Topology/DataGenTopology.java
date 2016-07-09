//package com.databricks.apps.logs.chapter1;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.Time;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import java.io.File;
//import java.util.*;
//
///**
// * Created by anshushukla on 23/06/15.
// */
//public class TestSTreaming {
//
//
//}


package Topology;


import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.Arrays;

public  class DataGenTopology {
    private DataGenTopology() {
    }


public static JavaDStream<String>  executetopo(JavaDStream<String> inputstream, String experiRunId)
{

            JavaDStream<String> words = inputstream.flatMap(
                    new FlatMapFunction<String, String>() {
                        @Override public Iterable<String> call(String x) {
                            String  msgcontent=MsgIdAddandRemove.getMessageContent(x);
                            Long id=MsgIdAddandRemove.getMessageId(x);

                            return MsgIdAddandRemove.addMessageId(Arrays.asList(msgcontent.split(",")), id);
                        }
                    });




return words;

}

}

/*

L   SEQUENCE   /Users/anshushukla/data/experi-smartplug-10min.csv   PLUG-1  1   /Users/anshushukla/data/output/temp
L   SEQUENCE   /Users/anshushukla/data/experi-sorted-data-taxi-less.csv    TAXI-1  1   /Users/anshushukla/data/output/temp
L   SEQUENCE   /Users/anshushukla/data/experi-sensercity-1o-min.csv    SYS-1  1   /Users/anshushukla/data/output/temp

*/