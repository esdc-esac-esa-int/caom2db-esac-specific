package esac.archive.ehst.dl.caom2.artifac.sync;

import ca.nrc.cadc.caom2.artifactsync.ArtifactStore;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.FileMetadata;

import java.beans.PropertyVetoException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessControlException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import esac.archive.ehst.dl.caom2.artifac.sync.checksums.EsacChecksumPersistance;
import esac.archive.ehst.dl.caom2.artifac.sync.checksums.db.ConfigProperties;

/**
 * ESAC Implementation of the ArtifactStore interface.
 *
 * This class interacts with the ESAC archive data web service to perform the artifact operations defined in ArtifactStore.
 *
 * @author jduran
 */
public class EsacArtifactStorage implements ArtifactStore {

    private static final Logger log = Logger.getLogger(EsacArtifactStorage.class.getName());

    protected static String J_FILES_LOCATION = null;
    protected static String L_FILES_LOCATION = null;
    protected static String X_FILES_LOCATION = null;
    protected static String Y_FILES_LOCATION = null;
    protected static String Z_FILES_LOCATION = null;
    protected static String N_FILES_LOCATION = null;
    protected static String O_FILES_LOCATION = null;
    protected static String I_FILES_LOCATION = null;
    protected static String W_FILES_LOCATION = null;
    protected static String U_FILES_LOCATION = null;
    protected static String V_FILES_LOCATION = null;
    protected static String F_FILES_LOCATION = null;

    public EsacArtifactStorage() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException,
            SQLException, PropertyVetoException {
        J_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.j");
        L_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.l");
        X_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.x");
        Y_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.y");
        Z_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.z");
        N_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.n");
        O_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.o");
        I_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.i");
        W_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.w");
        U_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.u");
        V_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.v");
        F_FILES_LOCATION = ConfigProperties.getInstance().getProperty("caom2.artifactsync.repository.hst.f");
    }

    @Override
    public boolean contains(URI artifactURI, URI checksum) throws TransientException {
        log.info("Entering contains method");
        if (artifactURI == null) {
            throw new IllegalArgumentException("ArtifactURI cannot be null");
        }
        init();
        boolean result = false;
        if (checksum != null) {
            result = EsacChecksumPersistance.getInstance().select(artifactURI, checksum);
        }
        return result;
    }

    public static String getJ() {
        return J_FILES_LOCATION;
    }
    public static String getL() {
        return L_FILES_LOCATION;
    }
    public static String getX() {
        return X_FILES_LOCATION;
    }
    public static String getY() {
        return Y_FILES_LOCATION;
    }
    public static String getZ() {
        return Z_FILES_LOCATION;
    }
    public static String getN() {
        return N_FILES_LOCATION;
    }
    public static String getO() {
        return O_FILES_LOCATION;
    }
    public static String getI() {
        return I_FILES_LOCATION;
    }
    public static String getW() {
        return W_FILES_LOCATION;
    }
    public static String getU() {
        return U_FILES_LOCATION;
    }
    public static String getV() {
        return V_FILES_LOCATION;
    }
    public static String getF() {
        return F_FILES_LOCATION;
    }

    private boolean saveFile(URI artifactURI, InputStream input) throws IOException, IllegalArgumentException {
        String path = null;
        try {
            path = parsePath(artifactURI.toString());
        } catch (NotValidInstrumentException e2) {
            e2.printStackTrace();
            return false;
        }

        boolean toBeCompressed = path.endsWith(".gz");

        Path pathToFile = Paths.get(path);
        pathToFile = pathToFile.getParent();

        log.info("START saving file '" + path + "'");

        try {
            Files.createDirectories(pathToFile);
        } catch (IOException e1) {
            log.error(e1.getMessage());
            throw e1;
        }
        File f = new File(path);
        if (!f.exists()) {
            f.createNewFile();
        }
        if (toBeCompressed) {
            FileOutputStream fos = null;
            GZIPOutputStream gzipOS = null;
            try {
                fos = new FileOutputStream(f);
                gzipOS = new GZIPOutputStream(fos);

                int read = 0;
                byte[] bytes = new byte[2048];

                log.info("writing file '" + path + "'");

                while ((read = input.read(bytes)) != -1) {
                    gzipOS.write(bytes, 0, read);
                }
                gzipOS.flush();

                log.info("file writen '" + path + "'");

            } catch (IOException ex) {
                log.error(ex.getMessage());
                throw ex;
            } finally {
                //                if (fos != null) {
                //                    fos.close();
                //                }
                if (gzipOS != null) {
                    gzipOS.close();
                }
            }

        } else {
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(f);
                int read = 0;
                byte[] bytes = new byte[2048];

                log.info("writing file '" + path + "'");

                while ((read = input.read(bytes)) != -1) {
                    fos.write(bytes, 0, read);
                }
                fos.flush();
                log.info("file writen '" + path + "'");

            } catch (IOException ex) {
                log.error(ex.getMessage());
                throw ex;
            } finally {
                if (fos != null) {
                    fos.close();
                }
            }
        }
        log.info("FINISH saving file '" + path + "'");

        return true;
    }

    private String parsePath(String artifact) throws NotValidInstrumentException {
        // modification -> mast:HST/product/x0bi0302t_dgr.fits
        String mast = artifact.split("[:]")[0];
        String rest = artifact.substring(mast.length() + 1);
        String[] parts = rest.split("[/]");
        String hst = parts[0];
        String product = parts[1];
        String name = parts[2];
        boolean toBeCompressed = name.endsWith(".fits");
        char first = name.charAt(0);
        String second = name.substring(1, 4);
        String third = name.substring(4, 6);

        String root = null;
        String filesLocation = null;
        if (first == 'j') {
            filesLocation = getJ();
        } else if (first == 'l') {
            filesLocation = getL();
        } else if (first == 'x') {
            filesLocation = getX();
        } else if (first == 'y') {
            filesLocation = getY();
        } else if (first == 'z') {
            filesLocation = getZ();
        } else if (first == 'n') {
            filesLocation = getN();
        } else if (first == 'o') {
            filesLocation = getO();
        } else if (first == 'i') {
            filesLocation = getI();
        } else if (first == 'w') {
            filesLocation = getW();
        } else if (first == 'u') {
            filesLocation = getU();
        } else if (first == 'v') {
            filesLocation = getV();
        } else if (first == 'f') {
            filesLocation = getF();
        } else {
            throw new NotValidInstrumentException("'" + first + "'" + " is not a valid instrument");
        }
        root = filesLocation.endsWith("/") ? filesLocation.substring(0, filesLocation.length() - 1) : filesLocation;
        String path = root + "/" + first + "/" + second + "/" + third + "/" + name;
        if (toBeCompressed) {
            path += ".gz";
        }
        return path;
    }

    private void init() {

    }

    private String calculateMD5Sum(InputStream data) throws UnsupportedOperationException, NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        StringBuffer hexString = new StringBuffer();
        try {
            byte[] dataBytes = new byte[1024];

            int nread = 0;
            while ((nread = data.read(dataBytes)) != -1) {
                md.update(dataBytes, 0, nread);
            }

            byte[] mdbytes = md.digest();

            // convert the byte to hex format method 1
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < mdbytes.length; i++) {
                sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
            }

            // convert the byte to hex format method 2
            for (int i = 0; i < mdbytes.length; i++) {
                String hex = Integer.toHexString(0xff & mdbytes[i]);
                if (hex.length() == 1)
                    hexString.append('0');
                hexString.append(hex);
            }
            hexString.insert(0, "md5:");
        } catch (FileNotFoundException e) {
            log.error(e.getMessage());
            throw e;
        }
        return hexString.toString();

    }

    @Override
    public void store(URI artifactURI, InputStream data, FileMetadata metadata)
            throws TransientException, UnsupportedOperationException, IllegalArgumentException, AccessControlException, IllegalStateException {
        log.info("Entering store method: artifactURI = " + artifactURI.toString() + " metadata = " + metadata);
        ByteArrayOutputStream baos = saveInputStream(data);

        if (artifactURI == null || metadata == null || metadata.getMd5Sum() == null) {
            throw new IllegalArgumentException("Neither ArtifactURI nor FileMetadata can be null");
        }
        init();
        URI checksum = null;
        try {
            checksum = new URI(metadata.getMd5Sum());
        } catch (URISyntaxException e1) {
            throw new TransientException("Unable to create URI from '" + metadata.getMd5Sum() + "'");
        }
        if (!contains(artifactURI, checksum)) {
            try {
                if (saveFile(artifactURI, new ByteArrayInputStream(baos.toByteArray()))) {
                    log.info("ArtifactURI = '" + artifactURI.toString() + "' saved locally");
                    String md5 = calculateMD5Sum(new ByteArrayInputStream(baos.toByteArray()));
                    URI md5Uri = new URI(md5);
                    String check = "md5:" + checksum.toString();
                    log.info("CHECKSUM: calculated md5 for " + artifactURI + " = " + md5);
                    log.info("CHECKSUM: expected md5 for   " + artifactURI + " = " + check);
                    if (checksum == null || md5.equals(check)) {
                        EsacChecksumPersistance.getInstance().upsert(artifactURI, md5Uri);
                    } else {
                        throw new TransientException("Mismatch between received checksum (" + check + ") and the calculeted one (" + md5 + ").");
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage());
                throw new AccessControlException(e.getCause().getMessage());
            }
        }

    }

    private ByteArrayOutputStream saveInputStream(InputStream data) {
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int len;
            while ((len = data.read(buffer)) > -1) {
                baos.write(buffer, 0, len);
            }
            baos.flush();
        } catch (Exception ex) {
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                }
                baos = null;
            }
        }
        return baos;
    }
}