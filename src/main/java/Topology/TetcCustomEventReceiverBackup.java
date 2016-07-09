package Topology;

import logging.BatchedFileLogging;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import utils.EventGen;
import utils.ISyntheticEventGen;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Created by anshushukla on 25/06/15.
 */


public class TetcCustomEventReceiverBackup extends Receiver<String> implements ISyntheticEventGen {

    EventGen eventGen;
    String datasetype;

    Random rand = new Random();
    public long msgId= 0;//rand.nextInt((10000 - 0) + 1) + 0;


    String filename ;
    BatchedFileLogging spoutlog;

    public TetcCustomEventReceiverBackup(String filename_, BatchedFileLogging spoutlog_, double _scalingFactor, String _datasetType) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        filename=filename_;
        spoutlog=spoutlog_;
        datasetype=_datasetType;
        eventGen=new EventGen(this,_scalingFactor);

    }

    @Override
    public StorageLevel storageLevel() {
        return  StorageLevel.MEMORY_AND_DISK_2();
    }

    @Override
    public void receive(List<String> event) {

        StringBuffer tuple=new StringBuffer();
        msgId++;
        for(String s:event)
        {
            tuple.append(s).append(",");

        }
        String s1=MsgIdAddandRemove.addMessageId(tuple.toString(),msgId);
//                    System.out.println("source,"+msgId+ ","+System.currentTimeMillis());
        try {
            spoutlog.batchLogwriter("spout,"+System.currentTimeMillis(),"MSGID,"+msgId);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);

        }

//      Thread.currentThread().sleep(100);
        store(s1);

    }

//    class Generator implements Runnable
//    {
//        TetcCustomEventReceiver j;
//        Scanner s;//
//        public Generator(TetcCustomEventReceiver _j, String logFile) throws Exception{
//            j = _j;
//            try {
//                s = new Scanner(new File(logFile)).useDelimiter(System.lineSeparator());
//
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//                throw e;
//            }
//        }
//
//
//        public void run() {
//
//            try {
//                int count=0;
//                while (s.hasNext()&& !j.isStopped()  && count++<5000)
//                {
//                    Thread.currentThread().sleep(1000);
//                    msgId++;
//                    String s1=MsgIdAddandRemove.addMessageId(s.next(),msgId);
////                    System.out.println("source,"+msgId+ ","+System.currentTimeMillis());
//                    spoutlog.batchLogwriter(System.currentTimeMillis(),",source,"+msgId);
//
//
////                    Thread.currentThread().sleep(100);
//                    j.store(s1);
//
//
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//                throw new RuntimeException(e);
//            }
//
//
//        }
//    }
//
//    private  Thread t;

    @Override
    public void onStart() {
        try {
            eventGen.launch(filename,datasetype);
        } catch (Exception e) {
            e.printStackTrace();
            throw  new RuntimeException(e);
        }
    }

    @Override
    public void onStop() {

        }
}
