package esac.archive.caom2.artifact.sync;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 *
 * @author jduran
 * @author Raul Gutierrez-Sanchez Copyright (c) 2014- European Space Agency
 *
 */
public class ConfigProperties {
	public static final String PROP_DB_HOST = "esac.tools.db.dbhost";
	public static final String PROP_DB_PORT = "esac.tools.db.dbport";
	public static final String PROP_DB_NAME = "esac.tools.db.dbname";
	public static final String PROP_DB_DRIVER = "esac.tools.db.driver";
	public static final String PROP_DB_USER = "esac.tools.db.username";
	public static final String PROP_DB_PASSWORD = "esac.tools.db.password";
	public static final String PROP_DB_SCHEMA = "esac.tools.db.dbschema";
	
    public static final String PROP_RESULTS_TABLENAME = "caom2.artifactsync.resultsTable.table";
    public static final String PROP_RESULTS_COLUMN_DATE = "caom2.artifactsync.resultsTable.columnDate";
    public static final String PROP_RESULTS_COLUMN_TOTALFILES = "caom2.artifactsync.resultsTable.columnTotalFiles";
    public static final String PROP_RESULTS_COLUMN_SUCCESSFILES = "caom2.artifactsync.resultsTable.columnSuccessFiles";
    public static final String PROP_RESULTS_COLUMN_ELAPSEDTIME = "caom2.artifactsync.resultsTable.columnElapsedTime";
    public static final String PROP_RESULTS_COLUMN_BYTES = "caom2.artifactsync.resultsTable.columnBytes";
    public static final String PROP_RESULTS_COLUMN_THREADS = "caom2.artifactsync.resultsTable.columnThreads";
    public static final String PROP_RESULTS_COLUMN_PERFORMANCE = "caom2.artifactsync.resultsTable.columnPerformance";
    public static final String PROP_RESULTS_COLUMN_UNIT = "caom2.artifactsync.resultsTable.columnUnit";
    public static final String PROP_RESULTS_COLUMN_MESSAGE = "caom2.artifactsync.resultsTable.columnMessage";
    public static final String PROP_RESULTS_PK = "caom2.artifactsync.resultsTable.pk";
    
    public static final String PROP_CHECKSUM_TABLENAME = "caom2.artifactsync.checksumTable.table";
    public static final String PROP_CHECKSUM_COLUMN_ARTIFACT = "caom2.artifactsync.checksumTable.columnArtifact";
    public static final String PROP_CHECKSUM_COLUMN_CHECKSUM = "caom2.artifactsync.checksumTable.columnChecksum";
    public static final String PROP_CHECKSUM_PK = "caom2.artifactsync.checksumTable.pk";

    private static boolean initialized = false;
    private static Properties prop;
    private static String collection = null;
    private static String pathToConfigFile = "";
    private static boolean forceShutdown = false;

    private static final Logger log = Logger.getLogger(ConfigProperties.class);

    private static ConfigProperties instance = null;

    public static ConfigProperties getInstance() {
        if (!initialized) {
            log.error("Error: Init() method must be called first.");
            System.exit(1);
        }

        if (instance == null) {
            instance = new ConfigProperties();
        }
        return instance;
    }

    public static void Init(String path, String c, boolean force) {
        log.info("Creating properties file");
        prop = new Properties();
        pathToConfigFile = path;
        collection = c;
        forceShutdown = force;
        initialized = true;
    }

    private ConfigProperties() {
        try {
            log.info("Props " + prop);
            //InputStream stream = getClass().getClassLoader().getResourceAsStream(pathToConfigFile);
            
            File configFile = new File(pathToConfigFile);
            
            if(!configFile.canRead()) {
            	throw new FileNotFoundException("property file " + pathToConfigFile + " not found in the classpath");
            }
            
            InputStream stream = new FileInputStream(configFile);
            
            log.info("Loading properties file '" + pathToConfigFile + "': " + stream);
            if (stream != null) {
                prop.load(stream);
            } 
            
            log.info("Properties file '" + pathToConfigFile + "' loaded");
        } catch (IOException e) {
            log.error("Error loading properties file '" + pathToConfigFile + "'");
            e.printStackTrace();
        }

    }

    public String getProperty(String property) {
        String result = null;
        result = prop.getProperty(property);
        if (result == null || result.equals("")) {
            log.error("Error reading properties file '" + pathToConfigFile + "'. There should be a parameter named " + property);
        }
        return result;
    }

    public void setProperty(String property, String value) {
        prop.setProperty(property, value);
    }

    /**
     * @return the collection
     */
    public static String getCollection() {
        return collection;
    }

    /**
     * @param collection
     *            the collection to set
     */
    public static void setCollection(String collection) {
        ConfigProperties.collection = collection;
    }

	public static boolean isForceShutdown() {
		return forceShutdown;
	}

}