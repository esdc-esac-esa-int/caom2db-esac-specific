package esac.esdc.ehst.dl.harvester.log.reader;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class LogReader {

    public static void analyze(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        System.out.println("Params are:");
        System.out.println("/tinputFileName");
        System.out.println("/toutputFileName");
        System.out.println("/tactionClassName");
        String inputFileName = args[0];
        String outputFileName = args[1];
        String actionClass = args[2];

        try (Stream<String> stream = Files.lines(Paths.get(inputFileName)); FileWriter writer = new FileWriter(outputFileName)) {
            Class<?> clazz = Class.forName(actionClass);
            IAction action = (IAction) clazz.newInstance();
            stream.filter(line -> action.criteria(line)).forEach(in -> {
                try {
                    String out = action.process(in);
                    writer.write(out);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
