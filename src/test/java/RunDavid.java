import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;
import org.springframework.core.io.ClassPathResource;

import java.io.File;


public class RunDavid {


public static void main (String args []) {



    System.out.println("R_HOME =" + System.getenv("R_HOME"));


    Rengine re = new Rengine (new String [] {"--vanilla"}, false, null);
    // Check if the session is working.
    if (!re.waitForR()) {
        return;
    }
    re.assign("x", new double[] {1.5, 2.5, 3.5});
    REXP result = re.eval("(sum(x))");
    System.out.println(result.asDouble());

    ClassPathResource rScript = new ClassPathResource("helloWorld.R");
    File f=new File("file:///Users/anshushukla/Downloads/tetc/ANSHU/sparkModBenchmark/src/test/resources/helloWorld.R");

    //        re.eval(String.format("source('%s')",rScript.getFile().getAbsolutePath()));
    re.eval(String.format("source('%s')",f));

    result = re.eval("greeting");
    System.out.println(result);
    System.out.println("Greeting from R: "+result.asString());

    re.end();
}}