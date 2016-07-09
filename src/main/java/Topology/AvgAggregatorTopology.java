package Topology;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import java.util.ArrayList;

public  class AvgAggregatorTopology {
    private AvgAggregatorTopology() {
    }


public static JavaDStream<String>  executetopo(JavaDStream<String> inputstream, final String experiRunId)
{

//
// Reduce function adding two integers, defined separately for clarity

    JavaPairDStream<String, String> pairs=
    inputstream.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
        @Override
        public Iterable<Tuple2<String, String>> call(String x) throws Exception {
            String  msgcontent=MsgIdAddandRemove.getMessageContent(x);
            Long id=MsgIdAddandRemove.getMessageId(x);
            String colArray[]=msgcontent.split(",");

            ArrayList<Tuple2<String,String>> t=new ArrayList<Tuple2<String,String>>();
            String [] emitlist = null;
            String [] fieldNames = null;
            Integer [] colIndexArr = null;
            int numCols=0;

            if(experiRunId.equals("TAXI")){
                numCols = 8;
                fieldNames = new String[numCols]; colIndexArr = new Integer[numCols];
//                emitlist=new String[numCols];
                fieldNames[0] = "trip_time_in_secs"; colIndexArr[0] = 4;
                fieldNames[1] = "trip_distance"; colIndexArr[1] = 5;
                fieldNames[2] = "fare_amount"; colIndexArr[2] = 11;
                fieldNames[3] = "surcharge"; colIndexArr[3] = 12;
                fieldNames[4] = "mta_tax"; colIndexArr[4] = 13;
                fieldNames[5] = "tip_amount"; colIndexArr[5] = 14;
                fieldNames[6] = "tolls_amount"; colIndexArr[6] = 15;
                fieldNames[7] = "total_amount"; colIndexArr[7] = 16;

                for(int i=0; i<numCols; i++){
                    t.add(new Tuple2(fieldNames[i],MsgIdAddandRemove.addMessageId(colArray[colIndexArr[i]], id)));
//                    emitlist[i]=fieldNames[i]+","+colArray[colIndexArr[i]];
                }
            }
            else if(experiRunId.equals("PLUG")){  //value, property  //SPECIAL CASE <0,val> <1,val> ...
//                emitlist=new String[1];
//                emitlist[0]=colArray[3]+","+colArray[2];
//                fieldNames = new String[numCols]; colIndexArr = new Integer[numCols];
                t.add(new Tuple2(colArray[3],MsgIdAddandRemove.addMessageId(colArray[2], id)));
            }
            else if(experiRunId.equals("SYS"))
            {
                numCols = 5;
                fieldNames = new String[numCols]; colIndexArr = new Integer[numCols];
//                emitlist=new String[numCols];
                fieldNames[0] = "temperature"; colIndexArr[0] = 4;
                fieldNames[1] = "humidity"; colIndexArr[1] = 5;
                fieldNames[2] = "light"; colIndexArr[2] = 6;
                fieldNames[3] = "dust"; colIndexArr[3] = 7;
                fieldNames[4] = "airquality_raw"; colIndexArr[4] = 8;
                for(int i=0; i<numCols; i++){
                    t.add(new Tuple2(fieldNames[i], MsgIdAddandRemove.addMessageId(colArray[colIndexArr[i]], id)));
                }
            }

            return t;
        }
    });

    JavaPairDStream<String, String> windowedWordCounts = pairs.reduceByKeyAndWindow(new Function2<String, String, String>() {

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
        }}, Durations.seconds(1),Durations.seconds(1));

    JavaDStream<String>  windowedWordCountsConverted=windowedWordCounts.map(new Function<Tuple2<String, String>, String>() {
        @Override
        public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
            String content=MsgIdAddandRemove.getMessageContent(stringStringTuple2._2());
            Long id=MsgIdAddandRemove.getMessageId(stringStringTuple2._2());
            return  MsgIdAddandRemove.addMessageId(stringStringTuple2._1()+""+content,id);
        }
    });


//    JavaDStream<String> BatchedAggregator=
//
//    words.map(new Function<String, String>() {
//        Map<String, Double> mapAggregatorValues=new HashMap<String, Double>();
//        Map<String, Integer> mapAggregatorCounters= new HashMap<String, Integer>(); //For maintaining individual counters
//        int batchCounterUpperLimit=60;
//        Integer currCounter=0;
//        Double avgValue=0.0;
//        @Override
//        public  String call(String x) throws Exception {
//            String  msgcontent=MsgIdAddandRemove.getMessageContent(x);
//            Long id=MsgIdAddandRemove.getMessageId(x);
//            String colArray[]=msgcontent.split(",");
//
////            this.mapAggregatorValues = new HashMap<String, Double>();
////            this.mapAggregatorCounters = new HashMap<String, Integer>();
////            this.batchCounterUpperLimit = 2;  //60
////            Integer currCounter=0;
////            Double avgValue=0.0;
//
//            String key = colArray[0];
//            Double value = Double.valueOf(colArray[1]);
//            Double tmpValue = (Double) mapAggregatorValues.get(key);
//            if(tmpValue == null){
//                mapAggregatorValues.put(key,value);
//                currCounter++;
//                mapAggregatorCounters.put(key, currCounter);
//            }
//            else{
//                mapAggregatorValues.put(key, tmpValue+value);
//                currCounter=mapAggregatorCounters.get(key);
//                currCounter++;
//                mapAggregatorCounters.put(key, currCounter);
//            }
//
//            if(currCounter == this.batchCounterUpperLimit)
//            {//System.out.println("Got counter!!!" + currCounter +"  " + key);
//                avgValue = mapAggregatorValues.get(key)/mapAggregatorCounters.get(key);
//
////                collector.emit(new Values(key + "," + avgValue, msgId));
//                mapAggregatorValues.put(key, null);
//                mapAggregatorCounters.put(key, null);
//                currCounter=0;
//                avgValue=0.0;
//
//            }
//
//            return  MsgIdAddandRemove.addMessageId(key+","+avgValue,id);
//        }
//    });


//    BatchedAggregator.filter(new Fun)


//  windowedWordCountsConverted.print();

    windowedWordCountsConverted.print();
  return windowedWordCountsConverted ;

}

}


/*

L   AvgAggregatorTopology   /Users/anshushukla/data/experi-smartplug-10min.csv   PLUG-1  1   /Users/anshushukla/data/output/temp
L   AvgAggregatorTopology   /Users/anshushukla/data/experi-sorted-data-taxi-less.csv    TAXI-1  0.0034   /Users/anshushukla/data/output/temp
L   AvgAggregatorTopology   /Users/anshushukla/data/experi-sensercity-1o-min.csv    SYS-1  1   /Users/anshushukla/data/output/temp

*/
