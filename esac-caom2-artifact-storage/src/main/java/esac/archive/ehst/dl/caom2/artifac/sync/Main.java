package esac.archive.ehst.dl.caom2.artifac.sync;

import ca.nrc.cadc.caom2.artifactsync.Caom2ArtifactSync;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import esac.archive.ehst.dl.caom2.artifac.sync.checksums.db.ConfigProperties;

/**
 *
 * @author jduran
 *
 */
public class Main {

    private static Logger log = Logger.getLogger(esac.archive.ehst.dl.caom2.artifac.sync.Main.class);

    public static void main(String[] args) throws Exception {
        ArgumentMap am = new ArgumentMap(args);
        if (am.isSet("d") || am.isSet("debug")) {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.artifac.sync", Level.DEBUG);
        } else if (am.isSet("v") || am.isSet("verbose")) {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.artifac.sync", Level.INFO);
        } else {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.artifac.sync", Level.WARN);
        }

        String configFile = am.getValue("configFile");
        String dbPass = am.getValue("dbPass");
        String collection = am.getValue("collection");

        if (collection == null) {
            log.error("missed --collection parameter");
            usage();
            System.exit(1);
        }
        if (configFile == null) {
            log.error("missed --configFile parameter");
            usage();
            System.exit(1);
        }
        if (dbPass == null) {
            log.error("missed --dbPass parameter");
            usage();
            System.exit(1);
        }
        ConfigProperties.Init(configFile, dbPass, collection);

        if (am.isSet("h") || am.isSet("help")) {
            usage();
            System.exit(0);
        }

        ca.nrc.cadc.caom2.artifactsync.Main.main(args);
    }

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: ").append(Caom2ArtifactSync.getApplicationName()).append(" <mode> [mode-args] --artifactStore=<fully qualified class name>");
        sb.append("\n      configFile=<path to configuration file>");
        sb.append("\n      dbPass=<password for database access>");
        sb.append("\n      collection=<collection>");
        sb.append("\n\n    use '").append(Caom2ArtifactSync.getApplicationName()).append(" <mode> <-h|--help>' to get help on a <mode>");
        sb.append("\n    where <mode> can be one of:");
        sb.append("\n        discover: Incrementally harvest artifacts");
        sb.append("\n        download: Download artifacts");
        sb.append("\n        validate: Discover missing artifacts and update the HarvestSkipURI table");
        sb.append("\n        diff: Discover and report missing artifacts");
        sb.append("\n\n    optional general args:");
        sb.append("\n        -v | --verbose");
        sb.append("\n        -d | --debug");
        sb.append("\n        -h | --help");
        sb.append("\n        --profile : Profile task execution");
        sb.append("\n\n    authentication:");
        sb.append("\n        [--netrc|--cert=<pem file>]");
        sb.append("\n        --netrc : read username and password(s) from ~/.netrc file");
        sb.append("\n        --cert=<pem file> : read client certificate from PEM file");

        log.warn(sb.toString());
    }

}
