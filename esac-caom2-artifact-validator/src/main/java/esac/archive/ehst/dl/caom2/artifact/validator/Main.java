package esac.archive.ehst.dl.caom2.artifact.validator;

import ca.nrc.cadc.caom2.artifactsync.Caom2ArtifactSync;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import esac.archive.ehst.dl.caom2.artifact.validator.checksums.EsacChecksumPersistance;
import esac.archive.ehst.dl.caom2.artifact.validator.checksums.EsacResultsPersistance;
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
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.artifact.validator", Level.DEBUG);
        } else if (am.isSet("v") || am.isSet("verbose")) {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.artifact.validator", Level.INFO);
        } else {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.artifact.validator", Level.WARN);
        }
        log.info("STARTING");

        String configFile = am.getValue("configFile");
        String dbPass = am.getValue("dbPass");
        String rootPath = am.getValue("rootPath");

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

        log.info("init");
        ConfigProperties.Init(configFile, dbPass, rootPath);

        IOFileFilter filter = new IOFileFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return true;
            }

            @Override
            public boolean accept(File file) {
                return true;
            }
        };

        log.info("Listing files in '" + ConfigProperties.getRootPath() + "'");
        Collection<File> files = FileUtils.listFiles(new File(ConfigProperties.getRootPath()), filter, TrueFileFilter.INSTANCE);

        //        Collection<File> files = FileUtils.listFiles(new File(ConfigProperties.getRootPath()), new WildcardFileFilter("*", IOCase.INSENSITIVE),
        //                new NotFileFilter(DirectoryFileFilter.DIRECTORY));
        EsacResultsPersistance.getInstance();

        log.info("Validating " + files.size() + " files in '" + ConfigProperties.getRootPath() + "'");
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
        String artifact = "mast:HST/product/" + file.getName();
        if (artifact.endsWith(".gz")) {
            artifact = artifact.replace(".gz", "");
        }
        URI artifactUri = new URI(artifact);
        InputStream input = EsacArtifactStorage.decompress(file);
        String calculatedChecksum = EsacArtifactStorage.calculateMD5Sum(input);
        input.close();
        URI calculatedChecksumUri = new URI(calculatedChecksum);
        boolean checksumExists = true;
        checksumExists = EsacChecksumPersistance.getInstance().select(artifactUri, calculatedChecksumUri);
        if (!checksumExists) {
            log.info("checksum (" + calculatedChecksum + ") doesn't exist for '" + artifactUri + "'");
            String expectedChecksum = EsacChecksumPersistance.getInstance().select(artifactUri);
            URI expectedChecksumUri = expectedChecksum == null ? null : new URI(expectedChecksum);
            EsacResultsPersistance.getInstance().upsert(artifactUri, expectedChecksumUri, calculatedChecksumUri);
            //                if (modify) {
            //                    EsacChecksumPersistance.getInstance().upsert(artifactUri, calculatedChecksumUri);
            //                }
        }
        return checksumExists;
    }

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: ").append(Caom2ArtifactSync.getApplicationName());
        sb.append("\n      configFile=path to configuration file");
        sb.append("\n      dbPass=password for database access");
        sb.append("\n      rootPath=root path to files to be validated");

        log.warn(sb.toString());
    }

}
