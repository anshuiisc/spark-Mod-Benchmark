package logging;

import utils.GlobalConstants;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by anshushukla on 20/05/15.
 */


public class BatchedFileLogging implements Serializable {

     int counter=0;
    String componentName;
    List<TupleType> batch = new ArrayList<TupleType>();
    FileWriter fstream;
      BufferedWriter out;
    String csvFileNameOut;
    int threshold; //Count of rows after which the map should be flushed to log file
    String logStringPrefix;


    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        stream.writeObject(batch);
        stream.writeInt(counter);
        stream.writeObject(csvFileNameOut);
        stream.writeInt(threshold);
        stream.writeObject(logStringPrefix);

    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        batch= (List<TupleType>) stream.readObject();
        counter=stream.readInt();
        csvFileNameOut = (String) stream.readObject();
        threshold=stream.readInt();
        logStringPrefix= (String) stream.readObject();

//        System.out.println("system"+csvFileNameOut+"----"+threshold+"-----"+logStringPrefix+"----"+counter);
        this.fstream = new FileWriter(csvFileNameOut,true);
        this.out = new BufferedWriter(fstream);
//


    }


    public BatchedFileLogging(String csvFileNameOut, String _componentName)
    {
    //
        Random rand=new Random();

        //
        componentName=_componentName;
        this.csvFileNameOut = csvFileNameOut;
        try {
            this.fstream = new FileWriter(this.csvFileNameOut,true);
            this.out = new BufferedWriter(fstream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.threshold = GlobalConstants.thresholdFlushToLog; //2000 etc
        try {
            this.logStringPrefix = InetAddress.getLocalHost().getHostName() + "," + Thread.currentThread().getName();
//            System.out.println(this.logStringPrefix+"changes done");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void batchLogwriter(String ts,String identifierData) throws IOException {



        if(this.threshold==1)
        {

//            System.out.println("going inside threshold as 1 if");
                String logStringPrefix1 = InetAddress.getLocalHost().getHostName() + "," + Thread.currentThread().getName();

                try {
                    this.out.write( logStringPrefix1 + "," + ts + "," + identifierData + "\n");
                } catch (IOException e) {
                    System.out.println("Excpetion while writing to file");
                    e.printStackTrace();
                    throw e;
                }


//            System.out.println("after batch writing");
            this.out.flush();
//            System.out.println("after the flush");
        }



       else if (counter<this.threshold)
        {
            System.out.print("data is inside hashmap with counter"+counter);
//            batch.put(ts,identifierData);
            batch.add(new TupleType(ts, identifierData));
            counter += 1;
        }
        else
        {

            for(TupleType tp : batch){

                String logStringPrefix1 = InetAddress.getLocalHost().getHostName() + "," + Thread.currentThread().getName();

                try {
                    this.out.write( logStringPrefix1 + "," + tp.ts + "," + tp.identifier + "\n");
                } catch (IOException e) {
                    System.out.println("Excpetion while writing to file");
                    e.printStackTrace();
                    throw e;
                }

            }
//            System.out.println("after batch writing");
            this.out.flush();
//            System.out.println("after the flush");
            batch.clear();
//            System.out.println("after the clear");

            counter = 1 ;
            batch.add(new TupleType(ts, identifierData));
        }
    }




    //mod


    public void flushLazy() throws IOException {
        this.out.flush();
//        count=1;
    }

    public void batchLogwriterLazy(String ts,String identifierData) throws IOException {



//        if(this.threshold==1)
//        {

//            System.out.println("going inside threshold as 1 if");
            String logStringPrefix1 = InetAddress.getLocalHost().getHostName() + "," + Thread.currentThread().getName();

            try {
                this.out.write( logStringPrefix1 + "," + ts + "," + identifierData + "\n");
            } catch (IOException e) {
                System.out.println("Excpetion while writing to file");
                e.printStackTrace();
                throw e;
            }


//            System.out.println("after batch writing");
//            this.out.flush();
//            System.out.println("after the flush");
//        }




    }
//mod end





}


class TupleType {
    String ts;
    String identifier;

    public TupleType(String ts, String identifier){
        this.ts = ts;
        this.identifier = identifier;
    }
}