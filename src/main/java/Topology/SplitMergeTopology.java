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


import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import utils.GlobalConstants;

public  class SplitMergeTopology {
    private SplitMergeTopology() {
    }


public static JavaDStream<String>  executetopo(JavaDStream<String> inputstream, final String  dataSetType)
{


//create stream 1
    JavaDStream<String>  Stream1= inputstream.filter(new Function<String, Boolean>() {
    @Override
    public Boolean call(String s) throws Exception {
        String content = MsgIdAddandRemove.getMessageContent(s);
        Long id = MsgIdAddandRemove.getMessageId(s);
        String colArray[] = content.split(",");
        int colIndex = 0;

        Boolean flag=false;
        if (dataSetType.equals("PLUG")) {
            colIndex = 0;
            String grepCol = colArray[colIndex];  //input.getString(colIndex);
            char[] charsGrepCol = grepCol.toCharArray();
            char lastChar = charsGrepCol[charsGrepCol.length - 1];

            if (GlobalConstants.isCharInRange(lastChar, '0', '3'))
                flag=true;
        }
        else if (dataSetType.equals("TAXI")) {
            colIndex = 1;
            String grepCol = colArray[colIndex];  //input.getString(colIndex);
            char[] charsGrepCol = grepCol.toCharArray();
            char lastChar = charsGrepCol[charsGrepCol.length - 1];

            if (GlobalConstants.isCharInRange(lastChar, 'A', 'F')){
                flag=true;
//                System.out.println("flag-"+flag);
            }
        } else if (dataSetType.equals("SYS")) {
            colIndex = 1;
            String grepCol = colArray[colIndex];  //input.getString(colIndex);
            char[] charsGrepCol = grepCol.toCharArray();
            char lastChar = charsGrepCol[charsGrepCol.length - 1];

            if (GlobalConstants.isCharInRange(lastChar, 'a', 'l'))
                flag=true;
        }
        else flag=false;

            return flag;
    }
});


//create stream 2
    JavaDStream<String>  Stream2= inputstream.filter(new Function<String, Boolean>() {
        @Override
        public Boolean call(String s) throws Exception {
            String content = MsgIdAddandRemove.getMessageContent(s);
            Long id = MsgIdAddandRemove.getMessageId(s);
            String colArray[] = content.split(",");
            int colIndex = 0;

            Boolean flag=false;
            if (dataSetType.equals("TAXI")) {
                colIndex = 1;
                String grepCol = colArray[colIndex];  //input.getString(colIndex);
                char[] charsGrepCol = grepCol.toCharArray();
                char lastChar = charsGrepCol[charsGrepCol.length - 1];

                if (GlobalConstants.isCharInRange(lastChar, '4', '6'))
                    flag=true;
            } else if (dataSetType.equals("PLUG")) {
                colIndex = 0;
                String grepCol = colArray[colIndex];  //input.getString(colIndex);
                char[] charsGrepCol = grepCol.toCharArray();
                char lastChar = charsGrepCol[charsGrepCol.length - 1];

                if (GlobalConstants.isCharInRange(lastChar, '0', '4'))
                    flag=true;
            } else if (dataSetType.equals("SYS")) {
                colIndex = 1;
                String grepCol = colArray[colIndex];  //input.getString(colIndex);
                char[] charsGrepCol = grepCol.toCharArray();
                char lastChar = charsGrepCol[charsGrepCol.length - 1];

                if (GlobalConstants.isCharInRange(lastChar, 'm', 'x'))
                    flag=true;
            }
            else flag=false;

            return flag;
        }
    });


//create stream 3
    JavaDStream<String>  Stream3= inputstream.filter(new Function<String, Boolean>() {
        @Override
        public Boolean call(String s) throws Exception {
            String content = MsgIdAddandRemove.getMessageContent(s);
            Long id = MsgIdAddandRemove.getMessageId(s);
            String colArray[] = content.split(",");
            int colIndex = 0;

            Boolean flag=false;
            if (dataSetType.equals("TAXI")) {
                colIndex = 1;
                String grepCol = colArray[colIndex];  //input.getString(colIndex);
                char[] charsGrepCol = grepCol.toCharArray();
                char lastChar = charsGrepCol[charsGrepCol.length - 1];

                if (GlobalConstants.isCharInRange(lastChar, '7', '9'))
                    flag=true;
            } else if (dataSetType.equals("PLUG")) {
                colIndex = 0;
                String grepCol = colArray[colIndex];  //input.getString(colIndex);
                char[] charsGrepCol = grepCol.toCharArray();
                char lastChar = charsGrepCol[charsGrepCol.length - 1];

                if (GlobalConstants.isCharInRange(lastChar, '5', '9'))
                    flag=true;
            } else if (dataSetType.equals("SYS")) {
                colIndex = 1;
                String grepCol = colArray[colIndex];  //input.getString(colIndex);
                char[] charsGrepCol = grepCol.toCharArray();
                char lastChar = charsGrepCol[charsGrepCol.length - 1];

                if (GlobalConstants.isCharInRange(lastChar, 'y', 'z'))
                    flag=true;
            }
            else flag=false;

            return flag;
        }
    });

//reverse elements in stream 1<tuples>

JavaDStream<String> ReverseColumnOrderStream1 = Stream1.map(new Function<String, String>() {
        @Override
        public String call(String s) throws Exception {
            String content=MsgIdAddandRemove.getMessageContent(s);
            Long id=MsgIdAddandRemove.getMessageId(s);
            String colArray[]=content.split(",");

            StringBuilder revStr = new StringBuilder();
            for(int i=colArray.length-1; i>0; i--){
                revStr.append(colArray[i]);
                revStr.append(",");
            }
            revStr.append(colArray[0]);
            String outRevStr = revStr.toString();
            return MsgIdAddandRemove.addMessageId(outRevStr+"stream1Reversed",id);
        }
    });

//NumericalOperation  on elements in stream 2<tuples>

    JavaDStream<String> NumericalOperationStream2=Stream2.map(new Function<String, String>() {
        @Override
        public String call(String s) throws Exception {
            String content=MsgIdAddandRemove.getMessageContent(s);
            Long id=MsgIdAddandRemove.getMessageId(s);
            String colArray[]=content.split(",");
            StringBuilder sb= new StringBuilder();
            int colIndex = 0;


            if(dataSetType.equals("PLUG")){

                String columnProperty = colArray[3]; //Property : Work = 0 and Load = 1
                String columnValueStr = colArray[2]; //Value of the associated property
                Double columnValue = Double.valueOf(columnValueStr);
                Double convertedValue = 0.0;
                if(columnProperty.equals("0")){ //If work convert kWh to kJoules
                    convertedValue = columnValue * 3600.0;
                }
                else{  //If load in Watt then convert to kiloWatt
                    convertedValue = columnValue / 1000.0;
                }
                colIndex = 2;
                colArray[colIndex] = String.valueOf(convertedValue);
                for(int i=0; i<colArray.length-1; i++){
                    sb.append(colArray[i]);
                    sb.append(",");
                }
                sb.append(colArray[colArray.length-1]);
            }

            else if(dataSetType.equals("TAXI")){
                colIndex =  4;  //4
                String column = colArray[colIndex];

                Double tripTimeInMins = Double.valueOf(column)/60.0;  //secs to mins
                colArray[colIndex] = String.valueOf(tripTimeInMins);
//                sb = new StringBuilder();
                for(int i=0; i<colArray.length-1; i++){
                    sb.append(colArray[i]);
                    sb.append(",");
                }
                sb.append(colArray[colArray.length-1]);
            }

            else if(dataSetType.equals("SYS")){
                colIndex = 4;  //4
                String column = colArray[colIndex];
                Double temperatureFahrenheit= (9.0/5.0)*Double.valueOf(column) + 32;  //Celsius to Fahr
                colArray[colIndex] = String.valueOf(temperatureFahrenheit);
//                sb = new StringBuilder();
                for(int i=0; i<colArray.length-1; i++){
                    sb.append(colArray[i]);
                    sb.append(",");
                }
                sb.append(colArray[colArray.length-1]);

            }

            return MsgIdAddandRemove.addMessageId(sb.toString()+"stream2Numericalop",id);
        }
    });

//AddBytes  to  elements in stream 3<tuples>
    JavaDStream<String> AppendBytesToColumnsStream3=Stream3.map(new Function<String, String>() {
        @Override
        public String call(String s) throws Exception {

            String content=MsgIdAddandRemove.getMessageContent(s);
            Long id=MsgIdAddandRemove.getMessageId(s);

            int size = content.getBytes().length;
            String outputRowString = content + "," + size;
            return MsgIdAddandRemove.addMessageId(outputRowString+"stream3addweight",id);
        }
    });


    JavaDStream<String> UnionStreams=NumericalOperationStream2.union(ReverseColumnOrderStream1).union(AppendBytesToColumnsStream3);


UnionStreams.print(1000);
    return  UnionStreams;


}

}



/*

L   SplitMergeTopology   /Users/anshushukla/data/experi-smartplug-10min.csv   PLUG-1  1   /Users/anshushukla/data/output/temp
L   SplitMergeTopology   /Users/anshushukla/data/experi-sorted-data-taxi-less.csv    TAXI-1  1   /Users/anshushukla/data/output/temp
L   SplitMergeTopology   /Users/anshushukla/data/experi-sensercity-1o-min.csv    SYS-1  1   /Users/anshushukla/data/output/temp

*/