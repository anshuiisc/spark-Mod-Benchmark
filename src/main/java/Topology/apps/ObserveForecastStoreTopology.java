package Topology.apps;

import Topology.MsgIdAddandRemove;
import logging.BatchedFileLogging;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Created by anshushukla on 29/06/15.
 */
public class ObserveForecastStoreTopology {
    public static JavaDStream<String> executetopo(final JavaDStream<String> inputstream, final String dataSetType, final BatchedFileLogging linReglog) throws IOException {


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

        JavaDStream<String> valid = parse.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String content = MsgIdAddandRemove.getMessageContent(s);
//                System.out.println("content in parse"+content);
                Long id = MsgIdAddandRemove.getMessageId(s);
                String colArray[] = content.split(",");
                int colIndex = 0;

                Boolean flag = true;
                if (dataSetType.equals("PLUG")) {
                    int propertyId = Integer.parseInt(colArray[3]);
                    if (propertyId == 0) { // kWh
                        if (isInvalid(colArray[2], 0, 10000)) flag = false; // ignore kWh data
                    } else if (propertyId == 1) { // Watts
                        if (isInvalid(colArray[2], 0, 1000)) flag = false; // ignore watts data
                    } else flag = false; // if propertyID != 0|1, ignore data
                } else if (dataSetType.equals("TAXI")) {
//                System.out.println("flag-"+flag);
                    throw new RuntimeException("\"Valid\" Bolt cannot be used with TAXI");
                } else if (dataSetType.equals("SYS")) {
                    //Logic for SYS
                    //timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
                    //2015-01-15T00:00:00.000Z,ci4q0adco000002t9qu491siy,-23.002739,-43.337678,34.1,45.3,0,1819.2,44
                    if (isInvalid(colArray[2], -90, 90)) flag = false; // ignore lat data
                    if (isInvalid(colArray[3], -180, 180)) flag = false; // ignore longi data
                    if (isInvalid(colArray[4], -40, 50)) flag = false; // ignore temp data 'C
                    if (isInvalid(colArray[5], 0, 100)) flag = false; // ignore humid data %
                    if (isInvalid(colArray[6], 0, 3000)) flag = false; // ignore light data LUX
                    if (isInvalid(colArray[7], 0, 2000)) flag = false; // ignore dust data pcs/238mL
                    if (isInvalid(colArray[8], 0, 300)) flag = false; // ignore pollu data mV
                }

                return flag;
            }
        });


        JavaDStream<String> interplolationStream =
                valid.mapToPair(new PairFunction<String, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(String s) throws Exception {
                        String content = MsgIdAddandRemove.getMessageContent(s);
                        Long id = MsgIdAddandRemove.getMessageId(s);
//                System.out.println("MSGID"+id);
                        String colArray[] = content.split(",");
//                StringBuffer insRowString;

                        if (dataSetType.equals("SYS")) {

                            StringBuffer  insRowString = new StringBuffer();
                            insRowString.append(colArray[0]).append(','); // retain timestamp since it is a str format
                            insRowString.append(colArray[1]).append(','); // retain src
                            insRowString.append(colArray[2]).append(','); // retain lat
                            insRowString.append(colArray[3]).append(','); // retain lon
                            insRowString.append(colArray[4]).append("#1").append(','); // ins temp
                            insRowString.append(colArray[5]).append("#1").append(','); // ins hum
                            insRowString.append(colArray[6]).append("#1").append(','); // ins light
                            insRowString.append(colArray[7]).append("#1").append(','); // ins dust
                            insRowString.append(colArray[8]).append("#1").append(','); // ins airQ
//                    System.out.println("valid"+insRowString);
                            return new Tuple2<Long, String>(id / 5, MsgIdAddandRemove.addMessageId(insRowString.toString(),id));
                        }
                        else
                            return null;
                        //to dp plug




                    }
                }).reduceByKey(new Function2<String, String, String>() {
                    @Override
                    public String call(String c1, String c2) throws Exception {

                        String s1 = MsgIdAddandRemove.getMessageContent(c1);
                        Long id1 = MsgIdAddandRemove.getMessageId(c1);
                        String s2 = MsgIdAddandRemove.getMessageContent(c2);

                        String colArray1[] = s1.split(",");
                        String colArray2[] = s2.split(",");

                        String ValandCount1_4[]=colArray1[4].split("#");
                        String ValandCount1_5[]=colArray1[5].split("#");
                        String ValandCount1_6[]=colArray1[6].split("#");
                        String ValandCount1_7[]=colArray1[7].split("#");
                        String ValandCount1_8[]=colArray1[8].split("#");

                        String ValandCount2_4[]=colArray2[4].split("#");
                        String ValandCount2_5[]=colArray2[5].split("#");
                        String ValandCount2_6[]=colArray2[6].split("#");
                        String ValandCount2_7[]=colArray2[7].split("#");
                        String ValandCount2_8[]=colArray2[8].split("#");

                        String[] temp=new String[2];
                        String[] hum=new String[2];
                        String[] light=new String[2];
                        String[] dust=new String[2];
                        String[] air=new String[2];

                        temp[0]     =Float.parseFloat(ValandCount1_4[0])+Float.parseFloat(ValandCount2_4[0])+"";
                        temp[1]     =Float.parseFloat(ValandCount1_4[1])+Float.parseFloat(ValandCount2_4[1])+"";

                        hum[0]     =Float.parseFloat(ValandCount1_5[0])+Float.parseFloat(ValandCount2_5[0])+"";
                        hum[1]     =Float.parseFloat(ValandCount1_5[1])+Float.parseFloat(ValandCount2_5[1])+"";

                        light[0]     =Float.parseFloat(ValandCount1_6[0])+Float.parseFloat(ValandCount2_6[0])+"";
                        light[1]     =Float.parseFloat(ValandCount1_6[1])+Float.parseFloat(ValandCount2_6[1])+"";

                        dust[0]     =Float.parseFloat(ValandCount1_7[0])+Float.parseFloat(ValandCount2_7[0])+"";
                        dust[1]     =Float.parseFloat(ValandCount1_7[1])+Float.parseFloat(ValandCount2_7[1])+"";

                        air[0]     =Float.parseFloat(ValandCount1_8[0])+Float.parseFloat(ValandCount2_8[0])+"";
                        air[1]     =Float.parseFloat(ValandCount1_8[1])+Float.parseFloat(ValandCount2_8[1])+"";


                        StringBuffer  insRowString = new StringBuffer();
                        insRowString.append(colArray1[0]).append(','); // retain timestamp since it is a str format
                        insRowString.append(colArray1[1]).append(','); // retain src
                        insRowString.append(colArray1[2]).append(','); // retain lat
                        insRowString.append(colArray1[3]).append(','); // retain lon

                        insRowString.append(temp[0]).append("#").append(temp[1]).append(','); // ins temp
                        insRowString.append(hum[0]).append("#").append(hum[1]).append(',');
                        insRowString.append(light[0]).append("#").append(light[1]).append(',');
                        insRowString.append(dust[0]).append("#").append(dust[1]).append(',');
                        insRowString.append(air[0]).append("#").append(air[1]).append(',');

//                System.out.println("validafter reduce bykey"+insRowString);
                        return MsgIdAddandRemove.addMessageId(insRowString.toString(),id1);

                    }
                }).map(new Function<Tuple2<Long,String>, String>() {
                    @Override
                    public String call(Tuple2<Long, String> longStringTuple2) throws Exception {

                        String s1 = MsgIdAddandRemove.getMessageContent(longStringTuple2._2());

                        Long id1 = MsgIdAddandRemove.getMessageId(longStringTuple2._2());
                        String colArray1[] = s1.split(",");



                        String ValandCount1_4[]=colArray1[4].split("#");
                        String ValandCount1_5[]=colArray1[5].split("#");
                        String ValandCount1_6[]=colArray1[6].split("#");
                        String ValandCount1_7[]=colArray1[7].split("#");
                        String ValandCount1_8[]=colArray1[8].split("#");

//                System.out.println("here"+ValandCount1_4[0]);
                        StringBuffer  insRowString = new StringBuffer();
                        insRowString.append(colArray1[0]).append(','); // retain timestamp since it is a str format
                        insRowString.append(colArray1[1]).append(','); // retain src
                        insRowString.append(colArray1[2]).append(','); // retain lat
                        insRowString.append(colArray1[3]).append(','); // retain lon

                        insRowString.append((Float.parseFloat(ValandCount1_4[0]) / Float.parseFloat(ValandCount1_4[1]))).append(','); // ins temp
                        insRowString.append((Float.parseFloat(ValandCount1_5[0]) / Float.parseFloat(ValandCount1_5[1]))).append(','); // ins temp
                        insRowString.append((Float.parseFloat(ValandCount1_6[0]) / Float.parseFloat(ValandCount1_6[1]))).append(','); // ins temp
                        insRowString.append((Float.parseFloat(ValandCount1_7[0]) / Float.parseFloat(ValandCount1_7[1]))).append(','); // ins temp
                        insRowString.append((Float.parseFloat(ValandCount1_8[0]) / Float.parseFloat(ValandCount1_8[1]))).append(','); // ins temp

//                System.out.println("valid after map"+insRowString);
                        return MsgIdAddandRemove.addMessageId(insRowString.toString(),id1);
                    }
                });

//                valid.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String x) throws Exception {
//                String  msgcontent= MsgIdAddandRemove.getMessageContent(x);
//                Long id=MsgIdAddandRemove.getMessageId(x);
//                Random r=new Random();
//                return  r.nextBoolean();
//            }


//        });

//        JavaPairDStream<Boolean, String> existsStream=valid.mapToPair(new PairFunction<String, Boolean, String>() {
//            @Override
//            public Tuple2<Boolean, String> call(String s) throws Exception {
//                return null;
//            }
//        }).cache();


        JavaDStream<String>  interunionStream=valid.union(interplolationStream);

//        JavaDStream<String>  data=

        interunionStream.foreachRDD(new Function<JavaRDD<String>, Void>()
        {



            @Override
            public Void call(JavaRDD<String> stringJavaRDD) throws Exception
            {
                final Long ts=System.currentTimeMillis();
                List<ArrayDeque<Float[]>> sysVals = new ArrayList<ArrayDeque<Float[]>>(5);
                SimpleRegression[] sysRegs = new SimpleRegression[5];
                long sysCount = 0;
                Iterator<String> tuple = stringJavaRDD.toLocalIterator();
                for(int i=0; i<5; i++) {
                    sysVals.add(new ArrayDeque<Float[]>(10));
                    sysRegs[i] = new SimpleRegression();
                }


                while (tuple.hasNext()) {

                    String  rowString=tuple.next();
                    String msgcontent = MsgIdAddandRemove.getMessageContent(rowString);
                    Long id = MsgIdAddandRemove.getMessageId(rowString);

                    String[] colArray = rowString.split(",");
                    if (dataSetType.equals("SYS")) {
                        //Logic for SYS
                        //timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
                        //2015-01-15T00:00:00.000Z,ci4q0adco000002t9qu491siy,-23.002739,-43.337678,34.1,45.3,0,1819.2,44
                        sysCount++;
                        for (int i = 0; i < 5; i++) {
                            float val = Float.parseFloat(colArray[4 + i]);
                            // add latest <attr, value> pair to list & regression
                            sysVals.get(i).add(new Float[]{(float) sysCount, val});
                            sysRegs[i].addData(sysCount, val);

                            // remove latest <attr, value> pair from list & regression
                            Float[] oldVal = sysVals.get(i).remove();
                            sysRegs[i].removeData(oldVal[0], oldVal[1]);

                            // make 10 predictions
                            StringBuffer predictions = new StringBuffer("attribute-").
                                    append(i).append(',').append(sysCount + 1).append(',');
                            for (int j = 1; j <= 10; j++) {
                                double pred = sysRegs[i].predict(sysCount + j);
                                predictions.append(pred).append(',');
                            }
                            // emit the predictions
                            linReglog.batchLogwriter("sink," + ts, "" + 111177777);
                        }
//                System.out.println("sink,"+ts +","+111177777);
                        /////

                    }
                }
                return null;
            }
        });



        JavaDStream<String>  toupdateStream=
                valid.filter(new Function<String, Boolean>() {
                    Random r=new Random();
                    @Override
                    public Boolean call(String s) throws Exception {
                        String msgcontent = MsgIdAddandRemove.getMessageContent(s);
                        Long id = MsgIdAddandRemove.getMessageId(s);

                        if((r.nextInt()&1)==0)
                            return  true;
                        else return false;
                    }
                });

        JavaDStream<String>  toinsertStream=
                valid.filter(new Function<String, Boolean>() {
                    Random r=new Random();
                    @Override
                    public Boolean call(String s) throws Exception {
                        String msgcontent = MsgIdAddandRemove.getMessageContent(s);
                        Long id = MsgIdAddandRemove.getMessageId(s);

                        if((r.nextInt()&1)==0)
                            return  true;
                        else return false;
                    }
                });


        JavaDStream<String> fetchStream = toupdateStream.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                String path = null;
                if (dataSetType.equals("PLUG")) {
                    path = "/tmp/tetc_fetch_plug.csv";
                } else if (dataSetType.equals("SYS")) {
                    path = "/tmp/tetc_fetch_sys.csv";


                    try (BufferedReader bf = Files.newBufferedReader(Paths.get(path), Charset.defaultCharset())) {
                        // FIXME: will this read the file that was created in constructor? Empty lines or exception?
                        bf.readLine();

                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Error in reader", e);

                    }
                }

                return path;
            }
        });


        JavaDStream<String> updatedStream = fetchStream.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                String path = null;
                if (dataSetType.equals("PLUG")) {
                    path = "/tmp/tetc_update_plug.csv";
                } else if (dataSetType.equals("SYS")) {
                    path = "/tmp/tetc_update_sys.csv";


                    try {
                        Files.write(Paths.get(path), s.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
                return MsgIdAddandRemove.addMessageId(s,11133333);
            }
        });


        JavaDStream<String> insertedStream = toinsertStream.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                String path = null;
                if (dataSetType.equals("PLUG")) {
                    path = "/tmp/tetc_insert_plug.csv";
                } else if (dataSetType.equals("SYS")) {
                    path = "/tmp/tetc_insert_sys.csv";


                    try {
                        Files.write(Paths.get(path), s.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
                return MsgIdAddandRemove.addMessageId(s,111444444);
//                return s;
            }


        });

        JavaDStream<String> fileunion=insertedStream.union(updatedStream);

//
//
//
//
//        //
//
//        JavaDStream<String> linregStream = interplolationStream.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterable<String> call(String s) throws Exception {
//                return null;
//            }
//        });
//
//
//        JavaDStream<String> unionStream = linregStream.union(updatedStream).union(insertedStream);
//

        // JavaDStream<String>  insert= existsStream2

//        // JavaDStream<String>  update= fetch
//
//                JavaDStream<String> interp=valid.map


        //        JavaDStream<String>   LinReg=interp

//        unionStream.print(100);
        return fileunion;
    }


    static boolean isInvalid(String numStr, float min, float max){
        float num = Float.parseFloat(numStr); if(num < min || num > max) return true; // ignore data
        return false;
    }
}



/*

L   ObserveForecastStoreTopology   /Users/anshushukla/data/experi-sensercity-1o-min.csv    SYS-1  1   /Users/anshushukla/data/output/temp

*/
