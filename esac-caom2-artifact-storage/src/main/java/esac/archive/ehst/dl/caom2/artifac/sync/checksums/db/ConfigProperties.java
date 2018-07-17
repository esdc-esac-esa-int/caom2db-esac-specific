package esac.archive.ehst.dl.caom2.artifac.sync.checksums.db;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 *
 * @author jduran
 *
 */
public class ConfigProperties {

    private static boolean initialized = false;
    private static Properties prop;
    private static String dbPass = "";
    private static String collection = null;
    private static String pathToConfigFile = "";

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

    public static void Init(String path, String pass, String c) {
        log.info("Creating properties file");
        prop = new Properties();
        dbPass = pass;
        pathToConfigFile = path;
        collection = c;
        initialized = true;
    }

    private ConfigProperties() {
        try {
            log.info("Props " + prop);
            InputStream stream = getClass().getClassLoader().getResourceAsStream(pathToConfigFile);
            log.info("Loading properties file '" + pathToConfigFile + "': " + stream);
            if (stream != null) {
                prop.load(stream);
            } else {
                throw new FileNotFoundException("property file " + pathToConfigFile + " not found in the classpath");
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

    public String getDbPass() {
        return dbPass;
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

}