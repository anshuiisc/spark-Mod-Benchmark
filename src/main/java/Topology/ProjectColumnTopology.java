package Topology;


import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

public  class ProjectColumnTopology {
    private ProjectColumnTopology() {
    }


public static JavaDStream<String>  executetopo(JavaDStream<String> inputstream, final String  dataSetType)
{



    JavaDStream<String>  ColumnProject=inputstream.map(new Function<String, String>() {
        @Override
        public String call(String s) throws Exception {
            String content=MsgIdAddandRemove.getMessageContent(s);
            Long id=MsgIdAddandRemove.getMessageId(s);
            String colArray[]=content.split(",");
            StringBuilder sb= new StringBuilder();
            int colIndex = 0;

            if(dataSetType.equals("PLUG")){
                colIndex=4;
                sb.append(colArray[colIndex]);
            }
            else if(dataSetType.equals("TAXI")){
                colIndex=4;
                sb.append(colArray[colIndex]);
            }
            else if(dataSetType.equals("SYS")){
                colIndex=4;
                sb.append(colArray[colIndex]);
            }
            return MsgIdAddandRemove.addMessageId(new String(sb),id);
        }
    });


    return ColumnProject;

}

}


/*

L   SEQUENCE   /Users/anshushukla/data/experi-smartplug-10min.csv   PLUG-1  1   /Users/anshushukla/data/output/temp
L   SEQUENCE   /Users/anshushukla/data/experi-sorted-data-taxi-less.csv    TAXI-1  1   /Users/anshushukla/data/output/temp
L   SEQUENCE   /Users/anshushukla/data/experi-sensercity-1o-min.csv    SYS-1  1   /Users/anshushukla/data/output/temp

*/
