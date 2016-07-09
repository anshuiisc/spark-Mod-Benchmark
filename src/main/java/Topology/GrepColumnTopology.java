


package Topology;


import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import utils.GlobalConstants;

  public  class GrepColumnTopology {
    private GrepColumnTopology() {
    }


public static JavaDStream<String>  executetopo(JavaDStream<String> inputstream, final String dataSetType)
{

    JavaDStream<String>  GrepWord = inputstream.map(new Function<String, String>() {
        @Override
        public String call(String s) throws Exception {
            String content=MsgIdAddandRemove.getMessageContent(s);
            Long id=MsgIdAddandRemove.getMessageId(s);
            String colArray[]=content.split(",");
            StringBuilder sb= new StringBuilder();
//            int colIndex = 0;


            if(dataSetType.equals("PLUG")) {
                int colIndex = 0;
                String grepCol = colArray[colIndex];
                char[] charsGrepCol = grepCol.toCharArray();
                char lastChar = charsGrepCol[charsGrepCol.length - 1];
                if (GlobalConstants.isCharInRange(lastChar, '0', '3')) {
                    sb.append(content);
                }
            }
            else if(dataSetType.equals("TAXI")){
                int colIndex = 1;  //4
                String grepCol = colArray[colIndex];
                char [] charsGrepCol = grepCol.toCharArray();
                char lastChar = charsGrepCol[charsGrepCol.length-1];

                if(GlobalConstants.isCharInRange(lastChar, 'A', 'F')){
                    sb.append(content);
                }
            }
            else if(dataSetType.equals("SYS")){
                int colIndex = 1;  //4
                String grepCol = colArray[colIndex];
                char [] charsGrepCol = grepCol.toCharArray();
                char lastChar = charsGrepCol[charsGrepCol.length-1];

                if(GlobalConstants.isCharInRange(lastChar, 'a', 'l')){
                    sb.append(content);
                }
            }
            return MsgIdAddandRemove.addMessageId(new String(sb),id);
        }
    });


    JavaDStream<String> FilteredGrep =  GrepWord.filter(new Function<String, Boolean>() {
        @Override
        public Boolean call(String s) throws Exception {
            if( s.split("@").length!=2)
            return false;
            else
                return true;
        }
    });

FilteredGrep.print(100);

return FilteredGrep;

}

}



/*

L   PROJECT   /Users/anshushukla/data/experi-smartplug-10min.csv   PLUG-1  1   /Users/anshushukla/data/output/temp
L   PROJECT   /Users/anshushukla/data/experi-sorted-data-taxi-less.csv    TAXI-0.0034  1   /Users/anshushukla/data/output/temp
L   PROJECT   /Users/anshushukla/data/experi-sensercity-1o-min.csv    SYS-1  1   /Users/anshushukla/data/output/temp

*/
