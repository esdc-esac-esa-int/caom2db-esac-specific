
package esac.archive.ehst.dl.caom2.repo.client.publications;

import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author jduran
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);

    public Main() {
    }

    public static void main(String[] args) {
        try {
            ArgumentMap am = new ArgumentMap(args);

            if (am.isSet("d") || am.isSet("debug")) {
                Log4jInit.setLevel("esac.archive.ehst.dl.caom2.repo.client.publications", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.reg.client", Level.DEBUG);
            } else if (am.isSet("v") || am.isSet("verbose")) {
                Log4jInit.setLevel("esac.archive.ehst.dl.caom2.repo.client.publications", Level.INFO);
            } else {
                Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
                Log4jInit.setLevel("esac.archive.ehst.dl.caom2.repo.client.publications", Level.WARN);
            }

            if (am.isSet("h") || am.isSet("help")) {
                usage();
                System.exit(0);
            }

            // TODO: implement useful command-line fatures here:

            // setup
            // am.getValue("resourceID")
            // am.getValue("collection")

            // get list
            // am.isSet("list")
            // am.getValue("maxrec")

            // get a single observation
            // am.getValue("observationID")
        } catch (Throwable uncaught) {
            log.error("uncaught exception", uncaught);
            System.exit(-1);
        }
    }

    private static void usage() {
        // TODO: add something useful
        log.warn("\n\nusage: esac-caom2-repo-publications [-v|--verbose|-d|--debug] [-h|--help] ...");
    }
}
