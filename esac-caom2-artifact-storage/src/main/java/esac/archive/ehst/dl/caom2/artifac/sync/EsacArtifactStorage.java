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
    private static String filesLocation = null;
    protected static final String FILES_LOCATION = "caom2.artifactsync.repository.root";

    public EsacArtifactStorage() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException,
            SQLException, PropertyVetoException {
        filesLocation = ConfigProperties.getInstance().getProperty(FILES_LOCATION);
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

    public static String getFilesLocation() {
        return filesLocation;
    }

    private boolean saveFile(URI artifactURI, InputStream input) throws IOException, IllegalArgumentException {
        String path = parsePath(artifactURI.toString());

        Path pathToFile = Paths.get(path);
        pathToFile = pathToFile.getParent();

        log.info("START saving file '" + path + "'");

        try {
            Files.createDirectories(pathToFile);
        } catch (IOException e1) {
            log.error(e1.getMessage());
            throw e1;
        }
        GZIPOutputStream gzipOS = null;
        FileOutputStream fos = null;
        File f = null;
        try {
            f = new File(path);
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
            if (fos != null) {
                try {
                    fos.close();
                    fos = null;
                    System.gc();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
            //            if (gzipOS != null) {
            //                try {
            //                    gzipOS.close();
            //                    gzipOS = null;
            //                    System.gc();
            //                } catch (IOException e) {
            //                    log.error(e.getMessage());
            //                }
            //            }
        }
        log.info("FINISH saving file '" + path + "'");

        return true;
    }

    // private String parsePath(String artifact) {
    // // example of artifact -> mast:HST/product/o6h086a6q/o6h086a6j_jit.fits
    // String mast = artifact.split("[:]")[0];
    // String rest = artifact.substring(mast.length() + 1);
    // String[] parts = rest.split("[/]");
    // String hst = parts[0];
    // String product = parts[1];
    // String name = parts[2];
    // char first = name.charAt(0);
    // String second = name.substring(1, 4);
    // String third = name.substring(4, 6);
    // String fileName = parts[3];
    //
    // String root = getFilesLocation().endsWith("/")
    // ? getFilesLocation().substring(0, getFilesLocation().length() - 1) :
    // getFilesLocation();
    // String path = root + "/" + mast + "/" + hst + "/" + product + "/" + first
    // + "/" + second + "/" + third + "/"
    // + fileName;
    // return path;
    // }

    private String parsePath(String artifact) {
        // modification -> mast:HST/product/x0bi0302t_dgr.fits
        String mast = artifact.split("[:]")[0];
        String rest = artifact.substring(mast.length() + 1);
        String[] parts = rest.split("[/]");
        String hst = parts[0];
        String product = parts[1];
        String name = parts[2];
        char first = name.charAt(0);
        String second = name.substring(1, 4);
        String third = name.substring(4, 6);

        String root = getFilesLocation().endsWith("/") ? getFilesLocation().substring(0, getFilesLocation().length() - 1) : getFilesLocation();
        String path = root + "/" + mast + "/" + hst + "/" + product + "/" + first + "/" + second + "/" + third + "/" + name;
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
            log.info("Checksum received form ArtifactURI = '" + artifactURI.toString() + "' is '" + checksum + "'");
        } catch (URISyntaxException e1) {
            throw new TransientException("Unable to create URI from '" + metadata.getMd5Sum() + "'");
        }
        if (!contains(artifactURI, checksum)) {
            log.info("ArtifactURI = '" + artifactURI.toString() + "' with checksum = '" + checksum + "' is not present locally");
            try {
                if (saveFile(artifactURI, new ByteArrayInputStream(baos.toByteArray()))) {
                    log.info("ArtifactURI = '" + artifactURI.toString() + "' saved locally");
                    String md5 = calculateMD5Sum(new ByteArrayInputStream(baos.toByteArray()));
                    URI md5Uri = new URI(md5);
                    String check = "md5:" + checksum.toString();
                    log.info("CHECKSUM: calculated md5 for " + artifactURI + " = " + md5);
                    log.info("CHECKSUM: expected md5 for " + artifactURI + " = " + check);
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