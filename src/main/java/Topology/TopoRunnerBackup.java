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
import java.util.Random;

public final class TopoRunnerBackup {
    private TopoRunnerBackup() {
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
        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
        final BatchedFileLogging spoutlog=new BatchedFileLogging(spoutLogFileName, "spout");
        final BatchedFileLogging sinklog=new BatchedFileLogging(sinkLogFileName, "sink");
        final BatchedFileLogging  linreglog=new BatchedFileLogging(sinkLogFileName+".Linreg", "sink");
        final BatchedFileLogging  linreglog_0=new BatchedFileLogging(sinkLogFileName+".Linreg_0", "sink");
        final BatchedFileLogging  linreglog_1=new BatchedFileLogging(sinkLogFileName+".Linreg_1", "sink");
//logging-code ends

        String datasetType = GlobalConstants.getDataSetTypeFromRunID(argumentClass.getExperiRunId());
        String datafilename= argumentClass.getInputDatasetPathName();//args[0];
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[4]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));


        JavaDStream<String> sourcestream1 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog,argumentClass.getScalingFactor(),datasetType));
        JavaDStream<String> sourcestream2 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog,argumentClass.getScalingFactor(),datasetType));
        JavaDStream<String> sourcestream3 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog,argumentClass.getScalingFactor(),datasetType));
        JavaDStream<String> sourcestream4 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog,argumentClass.getScalingFactor(),datasetType));
        JavaDStream<String> sourcestream5 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog,argumentClass.getScalingFactor(),datasetType));
        JavaDStream<String> sourcestream6 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog,argumentClass.getScalingFactor(),datasetType));
        JavaDStream<String> sourcestream7 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog,argumentClass.getScalingFactor(),datasetType));
        JavaDStream<String> sourcestream8 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog,argumentClass.getScalingFactor(),datasetType));
        JavaDStream<String> sourcestream9 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog,argumentClass.getScalingFactor(),datasetType));
        JavaDStream<String> sourcestream10 = ssc.receiverStream(
                new TetcCustomEventReceiver(datafilename,spoutlog,argumentClass.getScalingFactor(),datasetType));


        JavaDStream<String> sourcestream=sourcestream1.union(sourcestream2).union(sourcestream3).union(sourcestream4).union(sourcestream5)
                                            .union(sourcestream6).union(sourcestream7).union(sourcestream8).union(sourcestream10).union(sourcestream10);

        sourcestream.repartition(6).cache();

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

        sinkstream.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
    public Void call(JavaRDD<String> v1) throws Exception {
        final Long ts=System.currentTimeMillis();
//                final Long count= v1.count();
        v1.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {

//                System.out.println("sink,"+MsgIdAddandRemove.getMessageId(s)+ ","+ts);
                sinklog.batchLogwriter("sink,"+ts,""+MsgIdAddandRemove.getMessageId(s));
            }
        });
        return null;
    }
});

        sinkstream.print();
        ssc.start();

        ssc.awaitTermination();

    }
}
