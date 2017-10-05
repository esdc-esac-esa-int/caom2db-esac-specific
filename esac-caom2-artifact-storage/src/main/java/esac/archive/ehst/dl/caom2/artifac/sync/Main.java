package esac.archive.ehst.dl.caom2.artifac.sync;

import org.apache.log4j.Logger;

import ca.nrc.cadc.util.ArgumentMap;
import esac.archive.ehst.dl.caom2.artifac.sync.checksums.db.ConfigProperties;

public class Main {

	private static Logger log = Logger.getLogger(esac.archive.ehst.dl.caom2.artifac.sync.Main.class);

	public static void main(String[] args) throws Exception {
		ArgumentMap am = new ArgumentMap(args);
		String configFile = am.getValue("configFile");
		String dbPass = am.getValue("dbPass");

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
		ConfigProperties.Init(configFile, dbPass);

		if (am.isSet("h") || am.isSet("help")) {
			usage();
			System.exit(0);
		}

		ca.nrc.cadc.caom2.artifactsync.Main.main(args);
	}

	private static void usage() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n\nusage: esac-caom2-artifact-storage [-v|--verbose|-d|--debug] [-h|--help] ...");
		sb.append("\n     --configFile=<path to configuration file>");
		sb.append("\n     --dbPass=<password for database access>");
		sb.append("\n     --artifactStore=<fully qualified class name>");
		sb.append("\n     --database=<server.database.schema>");
		sb.append("\n     --collection=<collection> (currently ignored)");
		sb.append("\n     --threads=<number of threads to be used to import artifacts (default: 1)>");
		sb.append("\n\nOptional:");
		sb.append("\n     --dryrun : check for work but don't do anything");
		sb.append("\n     --batchsize=<integer> Max artifacts to check each iteration (default: 1000)");
		sb.append("\n     --continue : repeat the batches until no work left");
		sb.append("\n\nAuthentication:");
		sb.append("\n     [--netrc|--cert=<pem file>]");
		sb.append("\n     --netrc : read username and password(s) from ~/.netrc file");
		sb.append("\n     --cert=<pem file> : read client certificate from PEM file");

		log.warn(sb.toString());
	}

}
