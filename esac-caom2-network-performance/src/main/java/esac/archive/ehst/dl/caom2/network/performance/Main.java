package esac.archive.ehst.dl.caom2.network.performance;

import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import esac.archive.ehst.dl.caom2.network.performance.db.ConfigProperties;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author jduran
 *
 */
public class Main {

    private static Logger log = Logger.getLogger(esac.archive.ehst.dl.caom2.network.performance.Main.class);

    public static void main(String[] args) throws Exception {
        ArgumentMap am = new ArgumentMap(args);
        if (am.isSet("d") || am.isSet("debug")) {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.network.performance", Level.DEBUG);
        } else if (am.isSet("v") || am.isSet("verbose")) {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.network.performance", Level.INFO);
        } else {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.network.performance", Level.WARN);
        }

        String configFile = am.getValue("configFile");
        String dbPass = am.getValue("dbPass");
        String sStartDate = am.getValue("startDate");
        String sEndDate = am.getValue("endDate");

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
        
        Date startDate = null;
        Date endDate = null;
        if (sStartDate != null) {
        	try {
        		startDate = new SimpleDateFormat(EsacResultsPersistance.format).parse(sStartDate);
        	} catch(ParseException pe) {
        		log.error("not valid startDate: " + sStartDate);
                usage();
                System.exit(1);
        	}
        }
        if (sEndDate != null) {
        	try {
        		endDate = new SimpleDateFormat(EsacResultsPersistance.format).parse(sEndDate);
        	} catch(ParseException pe) {
        		log.error("not valid endDate: " + sEndDate);
                usage();
                System.exit(1);
        	}
        }

        NetworkPerformanceResult r = EsacResultsPersistance.getInstance().select(startDate, endDate);
        System.out.println(r);
    }

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: ").append("esac-caom2-network-performance").append(" <mode> [mode-args]");
        sb.append("\n      configFile=<path to configuration file>");
        sb.append("\n      dbPass=<password for database access>");
        sb.append("\nOPTIONAL");
        sb.append("\n      startDate=start date (format = 'yyyy/MM/dd HH:mm:SS\'");
        sb.append("\n      endDate=end date (format = 'yyyy/MM/dd HH:mm:SS'");

        log.warn(sb.toString());
    }

}
