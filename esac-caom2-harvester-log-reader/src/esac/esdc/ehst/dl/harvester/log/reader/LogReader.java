package esac.esdc.ehst.dl.harvester.log.reader;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class LogReader {

    public static void main(String[] args) {
        System.out.println("Params are:");
        System.out.println("/tinputFileName");
        System.out.println("/toutputFileName");
        String inputFileName = args[0];
        String outputFileName = args[1];

        try (Stream<String> stream = Files.lines(Paths.get(inputFileName));
                FileWriter writer = new FileWriter(outputFileName)) {

            stream.filter(line -> line.contains("failed to insert")).forEach(l -> {
                try {
                    writer.write(l);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
