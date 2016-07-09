import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

public class Temp {

    public static void main(String a[]) {
        RConnection connection = null;

        try {
            /* Create a connection to Rserve instance running on default port
             * 6311
             */
            connection = new RConnection();
 
            /* Note four slashes (\\\\) in the path */
            connection.eval("source('file:///Users/anshushukla/Downloads/tetc/ANSHU/sparkModBenchmark/src/test/resources/MyScript.R')");
            int num1=10;
            int num2=20;
            int sum=connection.eval("myAdd("+num1+","+num2+")").asInteger();
            System.out.println("The sum is=" + sum);

            connection.eval("source('file:///Users/anshushukla/Downloads/tetc/ANSHU/sparkModBenchmark/src/test/resources/helloWorld.R')");
            System.out.println(connection.eval("greeting").asString());


        } catch (RserveException e) {
            e.printStackTrace();
        } catch (REXPMismatchException e) {
            e.printStackTrace();
        }
    }
}