package esac.archive.ehst.dl.caom2.repo.client.publications;

import ca.nrc.cadc.util.Log4jInit;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.log4j.Level;

public class Main {

    public static void main(String[] args) {
        Log4jInit.setLevel("esac.archive.ehst.dl.caom2.repo.client.publications", Level.DEBUG);

        try {
            System.out.println(ProposalsReader.getInstance().read(args[0]));
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
