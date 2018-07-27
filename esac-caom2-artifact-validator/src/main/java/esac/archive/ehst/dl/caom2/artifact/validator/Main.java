package esac.archive.ehst.dl.caom2.artifact.validator;

import ca.nrc.cadc.caom2.artifactsync.Caom2ArtifactSync;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import esac.archive.ehst.dl.caom2.artifact.validator.checksums.EsacChecksumPersistance;
import esac.archive.ehst.dl.caom2.artifact.validator.checksums.db.ConfigProperties;

/**
 *
 * @author jduran
 *
 */
public class Main {

    private static Logger log = Logger.getLogger(esac.archive.ehst.dl.caom2.artifact.validator.Main.class);

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
        String rootPath = am.getValue("rootPath");

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
        if (rootPath == null) {
            log.error("missed --rootPath parameter");
            usage();
            System.exit(1);
        }

        if (am.isSet("h") || am.isSet("help")) {
            usage();
            System.exit(0);
        }

        ConfigProperties.Init(configFile, dbPass, collection, rootPath);

        log.info("Listing files in '" + ConfigProperties.getRootPath() + "'");
        Collection<File> files = FileUtils.listFiles(new File(ConfigProperties.getRootPath()), new WildcardFileFilter("*.*", IOCase.SENSITIVE),
                new NotFileFilter(DirectoryFileFilter.DIRECTORY));
        log.info("Validating files in '" + ConfigProperties.getRootPath() + "'");
        files.parallelStream().forEach(file -> {
            try {
                if (!validate(file)) {
                    log.error(file.getAbsolutePath());
                }
            } catch (IllegalArgumentException | SecurityException | IllegalAccessException | InvocationTargetException | NoSuchMethodException
                    | UnsupportedOperationException | NoSuchAlgorithmException | SQLException | PropertyVetoException | IOException | URISyntaxException e) {
                e.printStackTrace();
            }
        });

    }

    private static boolean validate(File file)
            throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException,
            PropertyVetoException, UnsupportedOperationException, NoSuchAlgorithmException, FileNotFoundException, IOException, URISyntaxException {
        log.info("Validating file in '" + file.getAbsolutePath() + "'");
        String artifactUri = "mast:HST/product/" + file.getName();
        String calculatedChecksum = EsacArtifactStorage.calculateMD5Sum(new FileInputStream(file));
        boolean checksumExists = true;
        synchronized (EsacChecksumPersistance.getInstance()) {
            checksumExists = EsacChecksumPersistance.getInstance().select(new URI(artifactUri), new URI(calculatedChecksum));
            if (!checksumExists) {
                boolean artifactExists = EsacChecksumPersistance.getInstance().select(new URI(artifactUri));
                if (artifactExists) {
                    EsacChecksumPersistance.getInstance().delete(new URI(artifactUri));
                }
            }
        }
        return checksumExists;
    }

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: ").append(Caom2ArtifactSync.getApplicationName());
        sb.append("\n      configFile=path to configuration file");
        sb.append("\n      dbPass=password for database access");
        sb.append("\n      collection=HST collection");
        sb.append("\n      rootPath=root path to files to be validated");

        log.warn(sb.toString());
    }

}
