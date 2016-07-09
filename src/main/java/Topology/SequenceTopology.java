package Topology;


import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

 public  class SequenceTopology {

    //dataset being static may be problem

    private SequenceTopology(String experiRunId) {
    }



public static JavaDStream<String>  executetopo(JavaDStream<String> inputstream, final String dataSetType)
{


    JavaDStream<String> ReverseColumnOrder = inputstream.map(new Function<String, String>() {
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
            String outRevStr = new String(revStr);
            return MsgIdAddandRemove.addMessageId(outRevStr,id);
        }
    });



    JavaDStream<String> NumericalOperation=ReverseColumnOrder.map(new Function<String, String>() {
        @Override
        public String call(String s) throws Exception {
            String content=MsgIdAddandRemove.getMessageContent(s);
            Long id=MsgIdAddandRemove.getMessageId(s);
            String colArray[]=content.split(",");
            StringBuilder sb= new StringBuilder();
            int colIndex = 0;


            if(dataSetType.equals("PLUG")){  //// ?

                String columnProperty = colArray[7-3-1]; //Property : Work = 0 and Load = 1
                String columnValueStr = colArray[7-2-1]; //Value of the associated property
                Double columnValue = Double.valueOf(columnValueStr);
                Double convertedValue = 0.0;
                if(columnProperty.equals("0")){ //If work convert kWh to kJoules
                    convertedValue = columnValue * 3600.0;
                }
                else{  //If load in Watt then convert to kiloWatt
                    convertedValue = columnValue / 1000.0;
                }
                colIndex = 7-2-1;
                colArray[colIndex] = String.valueOf(convertedValue);
                for(int i=0; i<colArray.length-1; i++){
                    sb.append(colArray[i]);
                    sb.append(",");
                }
                sb.append(colArray[colArray.length-1]);
            }

            else if(dataSetType.equals("TAXI")){
                colIndex = 17-4-1;  //4
                String column = colArray[colIndex];

                Double tripTimeInMins = Integer.valueOf(column)/60.0;  //secs to mins
                colArray[colIndex] = String.valueOf(tripTimeInMins);
//                sb = new StringBuilder();
                for(int i=0; i<colArray.length-1; i++){
                    sb.append(colArray[i]);
                    sb.append(",");
                }
                sb.append(colArray[colArray.length-1]);
            }

            else if(dataSetType.equals("SYS")){
                colIndex = 9-4-1;  //4
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

            return MsgIdAddandRemove.addMessageId(new String(sb),id);
        }
    });


    JavaDStream<String> AppendBytesToColumns=NumericalOperation.map(new Function<String, String>() {
        @Override
        public String call(String s) throws Exception {

            String content=MsgIdAddandRemove.getMessageContent(s);
            Long id=MsgIdAddandRemove.getMessageId(s);

            int size = content.getBytes().length;
            String outputRowString = content + "," + size;
            return MsgIdAddandRemove.addMessageId(outputRowString,id);
        }
    });


    return AppendBytesToColumns;
}



}

/*

L   SEQUENCE   /Users/anshushukla/data/experi-smartplug-10min.csv   PLUG-1  1   /Users/anshushukla/data/output/temp
L   SEQUENCE   /Users/anshushukla/data/experi-sorted-data-taxi-less.csv    TAXI-1  1   /Users/anshushukla/data/output/temp
L   SEQUENCE   /Users/anshushukla/data/experi-sensercity-1o-min.csv    SYS-1  1   /Users/anshushukla/data/output/temp

*/
