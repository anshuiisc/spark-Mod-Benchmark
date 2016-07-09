package Topology;


import Topology.apps.ObserveForecastStoreTopology;
import Topology.apps.ObserveForecastStoreTopologyPLUG;
import Topology.apps.TollTaxiTopology;
import factory.ArgumentClass;
import factory.ArgumentParser;
import logging.BatchedFileLogging;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import utils.GlobalConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public final class TopoRunner {
    private TopoRunner() {
    }

    public static void main(String[] args) throws IOException {


        //logging-code start
        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if(argumentClass == null){
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return ;
        }

        Random rand=new Random();
        String toponame=argumentClass.getTopoName();

        String logFilePrefix1 = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log1";
        String logFilePrefix2= argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log2";
        String logFilePrefix3 = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log3";
        String logFilePrefix4 = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log4";
        String logFilePrefix5 = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log5";
        String logFilePrefix6 = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log6";
        String logFilePrefix7 = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log7";
        String logFilePrefix8 = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log8";
        String logFilePrefix9 = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log9";
        String logFilePrefix10 = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log10";


        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix1;
        String sinkLogFileName_odd = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix2;

        String spoutLogFileName1 = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix1;
        String spoutLogFileName2 = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix2;
        String spoutLogFileName3 = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix3;
        String spoutLogFileName4 = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix4;
        String spoutLogFileName5 = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix5;
        String spoutLogFileName6 = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix6;
        String spoutLogFileName7 = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix7;
        String spoutLogFileName8 = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix8;
        String spoutLogFileName9 = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix9;
        String spoutLogFileName10 = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix10;


        final BatchedFileLogging spoutlog1=new BatchedFileLogging(spoutLogFileName1, "spout");
        final BatchedFileLogging spoutlog2=new BatchedFileLogging(spoutLogFileName2, "spout");
        final BatchedFileLogging spoutlog3=new BatchedFileLogging(spoutLogFileName3, "spout");
        final BatchedFileLogging spoutlog4=new BatchedFileLogging(spoutLogFileName4, "spout");
        final BatchedFileLogging spoutlog5=new BatchedFileLogging(spoutLogFileName5, "spout");
        final BatchedFileLogging spoutlog6=new BatchedFileLogging(spoutLogFileName6, "spout");
        final BatchedFileLogging spoutlog7=new BatchedFileLogging(spoutLogFileName7, "spout");
        final BatchedFileLogging spoutlog8=new BatchedFileLogging(spoutLogFileName8, "spout");
        final BatchedFileLogging spoutlog9=new BatchedFileLogging(spoutLogFileName9, "spout");
        final BatchedFileLogging spoutlog10=new BatchedFileLogging(spoutLogFileName10, "spout");




        final BatchedFileLogging sinklog=new BatchedFileLogging(sinkLogFileName, "sink");
        final BatchedFileLogging sinklog_odd=new BatchedFileLogging(sinkLogFileName_odd, "sink");

        final BatchedFileLogging  linreglog=new BatchedFileLogging(sinkLogFileName+".Linreg", "sink");
        final BatchedFileLogging  linreglog_0=new BatchedFileLogging(sinkLogFileName+".Linreg_0", "sink");
        final BatchedFileLogging  linreglog_1=new BatchedFileLogging(sinkLogFileName+".Linreg_1", "sink");
//logging-code ends

        String datasetType = GlobalConstants.getDataSetTypeFromRunID(argumentClass.getExperiRunId());
        String datafilename= argumentClass.getInputDatasetPathName();//args[0];
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[4]");


        if(toponame.equals("IdentityTopology"))
        {
            sparkConf.setExecutorEnv("SPARK_WORKER_CORES","1");


        }


        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));


        JavaDStream<String> sourcestream1 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog1,argumentClass.getScalingFactor(),datasetType));
        JavaDStream<String> sourcestream2 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog2,argumentClass.getScalingFactor(),datasetType));
//        JavaDStream<String> sourcestream3 = ssc.receiverStream(
//                new TetcCustomEventReceiver(datafilename,spoutlog3,argumentClass.getScalingFactor(),datasetType));
//        JavaDStream<String> sourcestream4 = ssc.receiverStream(
//                new TetcCustomEventReceiver(datafilename,spoutlog4,argumentClass.getScalingFactor(),datasetType));
//        JavaDStream<String> sourcestream5 = ssc.receiverStream(
//                new TetcCustomEventReceiver(datafilename,spoutlog5,argumentClass.getScalingFactor(),datasetType));
//        JavaDStream<String> sourcestream6 = ssc.receiverStream(
//                new TetcCustomEventReceiver(datafilename,spoutlog6,argumentClass.getScalingFactor(),datasetType));
//        JavaDStream<String> sourcestream7 = ssc.receiverStream(
//                new TetcCustomEventReceiver(datafilename,spoutlog7,argumentClass.getScalingFactor(),datasetType));
//        JavaDStream<String> sourcestream8 = ssc.receiverStream(
//                new TetcCustomEventReceiver(datafilename,spoutlog8,argumentClass.getScalingFactor(),datasetType));
//        JavaDStream<String> sourcestream9 = ssc.receiverStream(
//                new TetcCustomEventReceiver(datafilename,spoutlog9,argumentClass.getScalingFactor(),datasetType));
//        JavaDStream<String> sourcestream10 = ssc.receiverStream(
//                new TetcCustomEventReceiver(datafilename,spoutlog10,argumentClass.getScalingFactor(),datasetType));




        JavaDStream<String> sourcestream=sourcestream1.union(sourcestream2);///.union(sourcestream3).union(sourcestream4).union(sourcestream5)
                                            //.union(sourcestream6).union(sourcestream7).union(sourcestream8).union(sourcestream9).union(sourcestream10);

        sourcestream.cache();

        JavaDStream<String> sinkstream=null;




        if(toponame.equals("DataGenTopology"))
        {

        sinkstream = DataGenTopology.executetopo(sourcestream,datasetType);
        }
        else if(toponame.equals("ForkMergeTopology"))
        {
        sinkstream = ForkMergeTopology.executetopo(sourcestream,datasetType);
        }
        else if(toponame.equals("GrepColumnTopology"))
        {
        sinkstream = GrepColumnTopology.executetopo(sourcestream,datasetType);
        }
        else if(toponame.equals("IdentityTopology"))
        {

            sinkstream = IdentityTopology.executetopo(sourcestream,datasetType);
        }
        else if(toponame.equals("ProjectColumnTopology"))
        {
            sinkstream = ProjectColumnTopology.executetopo(sourcestream,datasetType);
        }
        else if(toponame.equals("SequenceTopology"))
        {
            sinkstream = SequenceTopology.executetopo(sourcestream,datasetType);
        }
        else if(toponame.equals("SplitMergeTopology"))
        {
            sinkstream = SplitMergeTopology.executetopo(sourcestream,datasetType);
        }
        else if(toponame.equals("AvgAggregatorTopology"))
        {
            sparkConf.setExecutorEnv("SPARK_WORKER_CORES","1");
            sinkstream = AvgAggregatorTopology.executetopo(sourcestream,datasetType);
        }
        else if(toponame.equals("TollTaxiTopology"))
        {
            sinkstream = TollTaxiTopology.executetopo(sourcestream, datasetType);
        }
        else if(toponame.equals("ObserveForecastStoreTopology"))
        {
            sinkstream = ObserveForecastStoreTopology.executetopo(sourcestream, datasetType, linreglog);
        }
        else if(toponame.equals("ObserveForecastStoreTopologyPLUG"))
        {
            sinkstream = ObserveForecastStoreTopologyPLUG.executetopo(sourcestream, datasetType, linreglog_0, linreglog_1);
        }

        else {
            System.out.println("Incorrect secong arg[1] argument for toponame ");
        }

//sinkstream.cache();
//sink logging



//        JavaDStream<String>  sink_even=sinkstream.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
////                System.out.println("checker"+s);
//                if((MsgIdAddandRemove.getMessageId(s)%2)==0)
//                    return true ;
//                else
//                    return false;
//            }
//        });
//        JavaDStream<String>  sink_odd=sinkstream.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
////                System.out.println("checker"+s);
//                if((MsgIdAddandRemove.getMessageId(s)%2)!=0)
//                    return true ;
//                else
//                    return false;
//            }
//        });


//        sink_odd.foreachRDD(new Function<JavaRDD<String>, Void>() {
//            @Override
//            public Void call(JavaRDD<String> v1) throws Exception {
//                final Long ts=System.currentTimeMillis();
////                final Long count= v1.count();
//                v1.foreach(new VoidFunction<String>() {
//                    @Override
//                    public void call(String s) throws Exception {
//
////                System.out.println("sink,"+MsgIdAddandRemove.getMessageId(s)+ ","+ts);
//                        sinklog_odd.batchLogwriter("sink,"+ts,""+MsgIdAddandRemove.getMessageId(s));
//                    }
//                });
//                return null;
//            }
//        });


//
//        sink_even.foreachRDD(new Function<JavaRDD<String>, Void>() {
//            @Override
//    public Void call(JavaRDD<String> v1) throws Exception {
//        final Long ts=System.currentTimeMillis();
////                final Long count= v1.count();
//        v1.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//
////                System.out.println("sink,"+MsgIdAddandRemove.getMessageId(s)+ ","+ts);
//                sinklog.batchLogwriter("sink,"+ts,""+MsgIdAddandRemove.getMessageId(s));
//            }
//        });
//        return null;
//    }
//});


  sinkstream.foreachRDD(new Function<JavaRDD<String>, Void>() {
//      List<String>  tupleBatch;
      @Override

    public Void call(JavaRDD<String> stringJavaRDD) throws Exception {
          final Long ts=System.currentTimeMillis();

        stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
            List<String>  tupleBatch=new ArrayList<String>();
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {
                while(stringIterator.hasNext()) {
                 String s=stringIterator.next();
//                    tupleBatch.add(String.valueOf(MsgIdAddandRemove.getMessageId(s)));
//                    sinklog_odd.batchLogwriter("sink,"+System.currentTimeMillis(),""+MsgIdAddandRemove.getMessageId(s));
                    sinklog_odd.batchLogwriterLazy("sink,"+ts,""+MsgIdAddandRemove.getMessageId(s));
                }
                sinklog_odd.flushLazy();
            }
//        for(String s:tupleBatch)
//            sinklog_odd.batchLogwriter(sinklog_odd.batchLogwriter("sink,"+System.currentTimeMillis(),""+MsgIdAddandRemove.getMessageId(s)););
        });

//        for(String s :tuple)
//    sinklog.batchLogwriter(sinklog_odd.batchLogwriter("sink,"+System.currentTimeMillis(),""+MsgIdAddandRemove.getMessageId(s)););
        return null;
    }
});


//        dstream.foreachRDD { rdd =>
//            rdd.foreachPartition { partitionOfRecords =>
//                val connection = createNewConnection()//create array list of empty
//                partitionOfRecords.foreach(record => connection.send(record)//list.add (MsgIdAddandRemove.getMessageId(s)) )
//                connection.close()//for all items in  liust :   call batch logger
//            }
//        }

        sinkstream.print();
        ssc.start();

        ssc.awaitTermination();

    }
}



//L   IdentityTopology   /Users/anshushukla/data/experi-smartplug-10min.csv   PLUG-1  0.01   /Users/anshushukla/data/output/temp
