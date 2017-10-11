package esac.archive.ehst.dl.caom2.artifac.sync;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessControlException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.artifactsync.ArtifactStore;
import ca.nrc.cadc.net.TransientException;
import esac.archive.ehst.dl.caom2.artifac.sync.checksums.EsacChecksumManagement;

/**
 * ESAC Implementation of the ArtifactStore interface.
 *
 * This class interacts with the ESAC archive data web service to perform the
 * artifact operations defined in ArtifactStore.
 *
 * @author jduran
 */
public class EsacArtifactStorage implements ArtifactStore {

	private static final Logger log = Logger.getLogger(EsacArtifactStorage.class.getName());

	String dataURL;

	public EsacArtifactStorage() throws IllegalArgumentException, SecurityException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException, SQLException, PropertyVetoException {
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
			result = EsacChecksumManagement.getInstance().select(artifactURI, checksum);
		}
		log.debug("after select " + result);
		return result;
	}

	@Override
	public void store(URI artifactURI, URI checksum, Long arg2, InputStream input) throws TransientException,
			UnsupportedOperationException, IllegalArgumentException, AccessControlException, IllegalStateException {
		log.info("Entering store method");
		if (artifactURI == null) {
			throw new IllegalArgumentException("ArtifactURI cannot be null");
		}
		init();
		if (!contains(artifactURI, checksum)) {
			try {
				if (saveFile(artifactURI, input)) {
					String md5 = calculateMD5Sum(parsePath(artifactURI.toString()));
					log.debug("CHECKSUM: calculated md5 from " + artifactURI + " = " + md5);
					log.debug("CHECKSUM: expected md5 from " + artifactURI + " = " + checksum);
					if (checksum == null || md5.equals(checksum.toString())) {
						EsacChecksumManagement.getInstance().upsert(artifactURI, checksum);
					}
				}
			} catch (Exception e) {
				throw new AccessControlException(e.getMessage());
			}
		}
	}

	private boolean saveFile(URI artifactURI, InputStream input) throws IOException {

		String path = parsePath(artifactURI.toString());
		log.debug("saveFile ********************** " + path);
		Path pathToFile = Paths.get(path);
		pathToFile = pathToFile.getParent();

		try {
			Files.createDirectories(pathToFile);
		} catch (IOException e1) {
			log.error(e1.getStackTrace());
			throw e1;
		}
		log.debug("saveFile ********************** directory " + pathToFile + " created");
		FileOutputStream fos = null;
		try {
			File f = new File(path);

			fos = new FileOutputStream(f);

			int read = 0;
			byte[] bytes = new byte[1024];

			while ((read = input.read(bytes)) != -1) {
				fos.write(bytes, 0, read);
			}
			log.debug("saveFile ********************** file " + f.getName() + " created");

		} catch (IOException ex) {
			log.error(ex.getStackTrace());
			System.exit(1);
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					log.error(e.getStackTrace());
					throw e;
				}
			}
		}
		return true;
	}

	private String parsePath(String artifact) {
		// example of artifact -> mast:HST/product/o6h086a6q/o6h086a6j_jit.fits
		String mast = artifact.split("[:]")[0];
		String rest = artifact.substring(mast.length() + 1);
		String[] parts = rest.split("[/]");
		String hst = parts[0];
		String product = parts[1];
		String name = parts[2];
		char first = name.charAt(0);
		String second = name.substring(1, 4);
		String third = name.substring(4, 6);
		String fileName = parts[3];
		String root = EsacChecksumManagement.getFilesLocation().endsWith("/")
				? EsacChecksumManagement.getFilesLocation().substring(0,
						EsacChecksumManagement.getFilesLocation().length() - 1)
				: EsacChecksumManagement.getFilesLocation();
		String path = root + "/" + mast + "/" + hst + "/" + product + "/" + first + "/" + second + "/" + third + "/"
				+ fileName;
		return path;
	}

	private void init() {

	}

	private String calculateMD5Sum(String path)
			throws UnsupportedOperationException, NoSuchAlgorithmException, IOException {
		MessageDigest md = MessageDigest.getInstance("MD5");

		StringBuffer hexString = new StringBuffer();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(path);

			byte[] dataBytes = new byte[1024];

			int nread = 0;
			while ((nread = fis.read(dataBytes)) != -1) {
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
		} catch (FileNotFoundException e) {
			throw e;
		} finally {
			if (fis != null) {
				fis.close();
			}
		}
		hexString.insert(0, "md5:");
		return hexString.toString();
	}
}