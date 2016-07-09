package Topology.apps;

import Topology.MsgIdAddandRemove;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by anshushukla on 29/06/15.
 */
public class TollTaxiTopology {
    public static JavaDStream<String> executetopo(JavaDStream<String> inputstream, String experiRunId) {



//        JavaDStream<String> parse=inputstream.map(new Function<String, String>() {
//            @Override
//            public String call(String x) throws Exception {
//                String  msgcontent= MsgIdAddandRemove.getMessageContent(x);
//                Long id=MsgIdAddandRemove.getMessageId(x);
//
//                String colArray[]=msgcontent.split(",");
//                StringBuffer toLowerRow = new StringBuffer();
//                for(String col : colArray){
//                    toLowerRow.append(col.toLowerCase()).append(',');   // FIXME: Adding an extra "," at the end
//                }
////                toLowerRow=toLowerRow.deleteCharAt(toLowerRow.length()-1);
//
//                msgcontent = toLowerRow.toString();
//
//                return MsgIdAddandRemove.addMessageId(msgcontent,id);
//            }
//        });

        JavaDStream<String> parse = inputstream.map(new Function<String, String>() {
            @Override
            public String call(String x) throws Exception {
                String msgcontent = MsgIdAddandRemove.getMessageContent(x);

                Long id = MsgIdAddandRemove.getMessageId(x);

                String colArray[] = msgcontent.split(",");
                StringBuffer toLowerRow = new StringBuffer();
                for (String col : colArray) {
                    toLowerRow.append(col.toLowerCase()).append(',');   // FIXME: Adding an extra "," at the end
                }
//                toLowerRow=toLowerRow.deleteCharAt(toLowerRow.length()-1);

                msgcontent = toLowerRow.toString();

                return MsgIdAddandRemove.addMessageId(msgcontent,id);
            }
        });



JavaDStream<String>  Grid=parse.map(new Function<String, String>() {
            @Override
            public String call(String x) throws Exception {
                String  msgcontent= MsgIdAddandRemove.getMessageContent(x);
                Long id=MsgIdAddandRemove.getMessageId(x);

                String [] colArray = msgcontent.split(",");
                //Sending a key value pair
                //Processing Logic for each message
                float lon1, lon2, lat1, lat2, tripDist;
                lon1 = lon2 = lat1 = lat2 = tripDist = 0;
                try {
                    lon1 = Float.parseFloat(colArray[6]);
                    lat1 = Float.parseFloat(colArray[7]);
                    lon2 = Float.parseFloat(colArray[8]);
                    lat2 = Float.parseFloat(colArray[9]);
                    tripDist = Float.parseFloat(colArray[5]);
                } catch(Exception e) {
                    // ignoring error and skip tuple
                    System.out.println("Error in parsing");
//                    return ;
                }

                // Manhattan Lat: 40.70 -- 40.85 ... 70-75, 75-80, 80-85
                // Manhattan Lon: -73.92 -- -74.02 .. 92-97, 97-02
                int latIndex = (int) ((lat2-40)*100-70)/5; // 0, 1, 3
                int lonIndex = (int) (((-lon2)-73)*100-92)/5; // 0, 1
                int gridId = latIndex * 3 + lonIndex;
                String outputString = "gridID,"+gridId ;
//                System.out.println(outputString);
                return MsgIdAddandRemove.addMessageId(outputString,id);
            }
        });

JavaDStream<String>  taxSumPercent=parse.map(new Function<String, String>() {
    float fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount;
    float taxTotal = 0;
    float tollTotal = 0;
            @Override
            public String call(String x) throws Exception {
                String  msgcontent= MsgIdAddandRemove.getMessageContent(x);
                Long id=MsgIdAddandRemove.getMessageId(x);
                String [] colArray = msgcontent.split(",");

                String taxiId = colArray[0];
                try {
                    fare_amount = Float.parseFloat(colArray[11]);
                    surcharge = Float.parseFloat(colArray[12]);
                    mta_tax = Float.parseFloat(colArray[13]);
                    tip_amount = Float.parseFloat(colArray[14]);
                    tolls_amount = Float.parseFloat(colArray[15]);
                    total_amount = Float.parseFloat(colArray[16]);
                } catch(Exception e) {
                    // ignoring error and skip tuple
                    System.out.println("Exception inside taxSumPercent");
                }

                taxTotal+= mta_tax;
                tollTotal += tolls_amount;
                float taxRate = (mta_tax + tolls_amount)/total_amount;

                String outStr = new StringBuffer("taxes,").append(taxTotal).append(",").append(tolls_amount).append(",").append(taxRate).append(",").append(taxiId).toString();
                return MsgIdAddandRemove.addMessageId(outStr,id);
            }
        }) ;



//        JavaDStream<String>   tollPercent=Grid .reduceByWindow(new Function2<String, String, String>() {
//                    @Override
//
//                    public String call(String s1, String s2) throws Exception {
////                System.out.println("print1-" + s1);
////                System.out.println("print2-" + s2);
//
//                        //s1     is   62@tolls_amount,0.00
//
//                        String msgcontent1 = MsgIdAddandRemove.getMessageContent(s1);
//                        String msgcontent2 = MsgIdAddandRemove.getMessageContent(s2);
//                        Long id1 = MsgIdAddandRemove.getMessageId(s1);
//                        Long id2 = MsgIdAddandRemove.getMessageId(s2);
////                System.out.println("msgcontent1"+msgcontent1);
////                System.out.println("msgcontent2"+msgcontent2);
//
//                        String[] msgcontent1Arr = msgcontent1.split(",");
//                        String[] msgcontent2Arr = msgcontent2.split(",");
//
//                        System.out.println(msgcontent1Arr[0] );
//                        System.out.println( msgcontent1Arr[1]);
//
//                        System.out.println(msgcontent2Arr[0] );
//                        System.out.println( msgcontent2Arr[1]);
//
//                        Double res1 = Double.parseDouble(msgcontent1Arr[1]) + Double.parseDouble(msgcontent2Arr[1]);
//
//                        if (id1 > id2)
//                            return MsgIdAddandRemove.addMessageId(msgcontent1Arr[0] + res1, id1);
//                        else
//                            return MsgIdAddandRemove.addMessageId(msgcontent1Arr[0] + res1, id2);
//
//
//                    }
//                }, Durations.seconds(2), Durations.seconds(2));

        JavaPairDStream<String,String> GridtoPair =
                Grid.mapToPair(new PairFunction<String, String,String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String content = MsgIdAddandRemove.getMessageContent(s);
                        Long id = MsgIdAddandRemove.getMessageId(s);
//                System.out.println("MSGID"+id);
                        String colArray[] = content.split(",");
//                StringBuffer insRowString;

                        String outString =new StringBuffer("Gridid,").append(colArray[1]).append(",1").toString();
                        System.out.println(outString);
                        return (new Tuple2<String, String>(colArray[1],MsgIdAddandRemove.addMessageId(1+"",id)));
                    }
                });

        JavaPairDStream<String, String> windowedGridCounts = GridtoPair.reduceByKeyAndWindow(new Function2<String, String, String>() {

            @Override public String call(String i1, String i2) {

                long  id1= MsgIdAddandRemove.getMessageId(i1);
                long  id2= MsgIdAddandRemove.getMessageId(i2);

                Float  v1= Float.parseFloat(MsgIdAddandRemove.getMessageContent(i1));
                Float  v2= Float.parseFloat(MsgIdAddandRemove.getMessageContent(i1));
                String res= String.valueOf(v1+v2);
                if(id1>id2) {
//                System.out.println(MsgIdAddandRemove.addMessageId(res, id1));

                    return MsgIdAddandRemove.addMessageId(res, id1);
                }
                else{
//                System.out.println(MsgIdAddandRemove.addMessageId(res, id2));
                    return MsgIdAddandRemove.addMessageId(res,id2);
                }
// return  Long.parseLong(v1 + v2);
            }}, Durations.seconds(60),Durations.seconds(60));


        JavaDStream<String>  tollPercent=windowedGridCounts.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String content=MsgIdAddandRemove.getMessageContent(stringStringTuple2._2());
                Long id=MsgIdAddandRemove.getMessageId(stringStringTuple2._2());

                System.out.println("windowedWordCountsConverted"+MsgIdAddandRemove.addMessageId(stringStringTuple2._1()+"#"+content,id));
                return  MsgIdAddandRemove.addMessageId(stringStringTuple2._1()+"#"+content,id);
            }
        });


        JavaDStream<String> underPay=
        taxSumPercent.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String x) throws Exception {

                String  msgcontent= MsgIdAddandRemove.getMessageContent(x);
                Long id=MsgIdAddandRemove.getMessageId(x);
                String [] colArray = msgcontent.split(",");

                String taxRate = colArray[3];
                if(Float.parseFloat(taxRate)<0.05)
                    return true;
                else
                return false;
            }
        }).map(new Function<String, String>() {
            @Override
            public String call(String x) throws Exception {
                String  msgcontent= MsgIdAddandRemove.getMessageContent(x);
                Long id=MsgIdAddandRemove.getMessageId(x);
                String [] colArray = msgcontent.split(",");

                String underStr =  new StringBuffer("underpayment,").append(colArray[4]).append(",").append(colArray[3]).toString();

                return  MsgIdAddandRemove.addMessageId(underStr,id);
            }
        });


//        JavaDStream<String>  underpay=taxSumPercent.map(new Function<String, String>() {
//            @Override
//            public String call(String x) throws Exception {
//                String  msgcontent= MsgIdAddandRemove.getMessageContent(x);
//                Long id=MsgIdAddandRemove.getMessageId(x);
//
//
//                return MsgIdAddandRemove.addMessageId(msgcontent,id);
//            }
//        });
//
//
////        tollPercent.window(1);
//        JavaDStream<String> sinkCOPY=  taxSumPercent.union(underpay).window(Durations.seconds(2),Durations.seconds(2)).union(tollPercent);

        JavaDStream<String> UnionStream=underPay.union(tollPercent).union(taxSumPercent);

        UnionStream.print(1000);

        return  UnionStream;
    }
}



/*

L   TollTaxiTopology   /Users/anshushukla/data/experi-sorted-data-taxi-less.csv    TAXI-1  0.0034   /Users/anshushukla/data/output/temp

*/
