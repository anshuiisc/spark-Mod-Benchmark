//import org.rosuda.JRI.Rengine;
//import org.rosuda.JRI.REXP;
//import org.springframework.core.io.ClassPathResource;
//
//public class HelloRWorld2 {
//    Rengine rengine; // initialized in constructor or autowired
//
//    public void helloRWorld() {
//        ClassPathResource rScript = new ClassPathResource("helloWorld.R");
//        rengine.eval(String.format("source('%s     rScript.getFile().getAbsolutePath()));
//                REXP result = rengine.eval("greeting     System.out.println("Greeting R: "+result.asString());
//    }
//}