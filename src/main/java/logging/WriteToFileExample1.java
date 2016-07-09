package logging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by anshushukla on 20/05/15.
 */
public class WriteToFileExample1 {

    public static void main(String[] args) {
        try {

            String content = "This is the content to write into file kdsbm cnm,c, dccd";

            File file = new File("/Users/anshushukla/Downloads/com-tarun/filename.txt");

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();

            System.out.println("Done");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
