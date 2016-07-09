package Topology;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

public  class IdentityTopologyBackup {
  private IdentityTopologyBackup() {
  }


  public static JavaDStream<String> executetopo(JavaDStream<String> inputstream, String experiRunId) {



      JavaDStream<String> words=inputstream.map(new Function<String, String>() {
          @Override
          public String call(String s) throws Exception {
              return s+"!!!!!!";
          }
      });


      return words;
  }
}



/*

L   IdentityTopology   /Users/anshushukla/data/experi-smartplug-10min.csv   PLUG-1  1   /Users/anshushukla/data/output/temp
L   IdentityTopology   /Users/anshushukla/data/experi-sorted-data-taxi-less.csv    TAXI-1  1   /Users/anshushukla/data/output/temp
L   IdentityTopology   /Users/anshushukla/data/experi-sensercity-1o-min.csv    SYS-1  1   /Users/anshushukla/data/output/temp

*/