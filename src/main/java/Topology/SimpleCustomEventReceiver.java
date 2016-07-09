package Topology;

import logging.BatchedFileLogging;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by anshushukla on 23/06/15.
 */

public class SimpleCustomEventReceiver extends Receiver<String> {

    Random rand = new Random();
    public long msgId= rand.nextInt((10000 - 0) + 1) + 0;


String filename ;
BatchedFileLogging spoutlog;

    public SimpleCustomEventReceiver(String filename_, BatchedFileLogging spoutlog_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        filename=filename_;
        spoutlog=spoutlog_;

    }



    @Override
    public StorageLevel storageLevel() {
        return  StorageLevel.MEMORY_AND_DISK_2();
    }

    class Generator implements Runnable
    {
        SimpleCustomEventReceiver j;
        Scanner s;//
        public Generator(SimpleCustomEventReceiver _j, String logFile) throws Exception{
            j = _j;
            try {
                s = new Scanner(new File(logFile)).useDelimiter(System.lineSeparator());

            } catch (FileNotFoundException e) {
                e.printStackTrace();
                throw e;
            }
        }


        public void run() {

            try {
                int count=0;
                while (s.hasNext()&& !j.isStopped()  && count++<5000)
                {
                    Thread.currentThread().sleep(1000);
                    msgId++;
                    String s1=MsgIdAddandRemove.addMessageId(s.next(),msgId);
//                    System.out.println("source,"+msgId+ ","+System.currentTimeMillis());
                    spoutlog.batchLogwriter("spout,"+System.currentTimeMillis(),",source,"+msgId);


//                    Thread.currentThread().sleep(100);
                    j.store(s1);


                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }


        }
    }

    private  Thread t;
    @Override
    public void onStart() {
        try {
            t=new Thread(new Generator(this,filename));
            t.start();
        } catch (Exception e) {

            e.printStackTrace();
            throw  new RuntimeException(e);
        }


    }

    @Override
    public void onStop() {
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw  new RuntimeException(e);
        }

    }
}
