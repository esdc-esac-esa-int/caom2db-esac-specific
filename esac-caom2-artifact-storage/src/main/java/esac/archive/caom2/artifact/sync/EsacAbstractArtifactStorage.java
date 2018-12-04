package esac.archive.caom2.artifact.sync;

import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.FileMetadata;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.AccessControlException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import esac.archive.caom2.artifact.sync.ConfigProperties;
import esac.archive.caom2.artifact.sync.EsacResultsPersistance;
import esac.archive.caom2.artifact.sync.checksum.EsacChecksumPersistance;
import esac.archive.caom2.artifact.sync.db.JdbcSingleton;

/**
 * ESAC Implementation of the ArtifactStore interface.
 *
 * This class interacts with the ESAC archive data web service to perform the artifact operations defined in ArtifactStore.
 *
 * @author jduran
 * @author Raul Gutierrez-Sanchez Copyright (c) 2014- European Space Agency
 */
public abstract class EsacAbstractArtifactStorage implements ArtifactStore {

    private static final Logger log = Logger.getLogger(EsacAbstractArtifactStorage.class.getName());

    protected static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    protected static final DecimalFormat decimalFormat = new DecimalFormat(".##");

    /**
     * Checks for artifact existence.
     *
     * @param artifactURI
     *            The artifact identifier.
     * @param checksum
     *            The checksum of the artifact.
     * @return True in the artifact exists with the given checksum.
     *
     * @throws UnsupportedOperationException
     *             If the artifact uri cannot be resolved.
     * @throws UnsupportedOperationException
     *             If the checksum algorith is not supported.
     * @throws IllegalArgumentException
     *             If an aspect of the artifact uri is incorrect.
     * @throws AccessControlException
     *             If the calling user is not allowed to perform the query.
     * @throws TransientException
     *             If an unexpected runtime error occurs.
     * @throws RuntimeException
     *             If an unrecovarable error occurs.
     */
    @Override
    public boolean contains(URI artifactURI, URI checksum) throws TransientException {
        log.info("******************** Entering contains method");
        if (artifactURI == null) {
            log.error("Artifact URI is null");
            return false;
        }

        boolean result = false;
        if (checksum != null) {
            try {
				result = EsacChecksumPersistance.getInstance().select(artifactURI, checksum);
			} catch (SQLException e) {
	            e.printStackTrace();
	            System.exit(2);
			}
        }

        log.info("******************** Leaving contains method for artifact: " + artifactURI.toString() + " = " + result);
        return result;
    }

    /**
     * Saves an artifact. The artifact will be replaced if artifact already exists with a different checksum.
     *
     * @param artifactURI
     *            The artifact identifier.
     * @param data
     *            The artifact data.
     * @param metadata
     *            Artifact metadata, including md5sum, contentLength and contentType
     *
     * @throws UnsupportedOperationException
     *             If the artifact uri cannot be resolved.
     * @throws UnsupportedOperationException
     *             If the checksum algorith is not supported.
     * @throws IllegalArgumentException
     *             If an aspect of the artifact uri is incorrect.
     * @throws AccessControlException
     *             If the calling user is not allowed to upload the artifact.
     * @throws IllegalStateException
     *             If the artifact already exists.
     * @throws TransientException
     *             If an unexpected runtime error occurs.
     * @throws RuntimeException
     *             If an unrecovarable error occurs.
     */
    @Override
    public void store(URI artifactURI, InputStream data, FileMetadata metadata)
            throws TransientException, UnsupportedOperationException, IllegalArgumentException, AccessControlException, IllegalStateException {
        log.info("******************** Entering store method");
        //ByteArrayOutputStream baos = saveInputStream(data);

        if (artifactURI == null || metadata == null || metadata.getMd5Sum() == null) {
            log.error("Neither ArtifactURI nor FileMetadata can be null");
            return;
        }
        if (data == null) {
            log.error("Data InputStream is null");
            return;
        }

        URI checksum = null;
        try {
            checksum = new URI(metadata.getMd5Sum());
        } catch (URISyntaxException e1) {
            log.error("Unable to create URI from '" + metadata.getMd5Sum() + "'");
            return;
        }
        if (checksum != null) {
        	Connection con = null;
            try {
                String metadataMD5 = "md5:" + checksum.toString();
                if (!contains(artifactURI, checksum)) {
                    log.info("******************** Saving " + artifactURI + " = " + metadataMD5 + " in the database");
                	con = JdbcSingleton.getInstance().getConnection();
                    saveFile(artifactURI, data, metadataMD5);
                    EsacChecksumPersistance.getInstance().upsert(artifactURI, checksum, con);
                }else {
                	log.info(artifactURI + " already in the system with " + checksum);
                }
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
                System.exit(2);
            } finally {
                try {
                    if (con != null) {
                        try {
							con.close();
						} catch (SQLException e) {
							e.printStackTrace();
							System.exit(2);
						}
                    }
                    if (data != null) {
                        data.close();
                    }
                } catch (IOException e) {
                    log.error(e.getMessage());
                    e.printStackTrace();
                    System.exit(2);
                }
            }
        }
        log.info("******************** Leaving store method");
    }

    /**
     * Get the list of all artifacts in a certain archive.
     *
     * @param archive
     *            The archive on which to search for files.
     * @return A list of archive metadata objects
     * @throws TransientException
     * @throws UnsupportedOperationException
     * @throws AccessControlException
     */
    @Override
    public Set<ArtifactMetadata> list(String archive) throws TransientException, UnsupportedOperationException, AccessControlException {
        log.info("******************** Entering list method for archive = '" + archive + "'");
        Set<ArtifactMetadata> artifactMetadataSet = new HashSet<ArtifactMetadata>();
        String sql = "SELECT a.artifact, a.checksum FROM caom2.checksums a";
        log.info(sql);
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            stmt.setFetchSize(1000);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                ArtifactMetadata am = new ArtifactMetadata();
                am.artifactURI = rs.getString(1);
                am.storageID = toStorageID(am.artifactURI);
                am.checksum = rs.getString(2);
                artifactMetadataSet.add(am);
            }
        } catch (SQLException | PropertyVetoException ex) {
            artifactMetadataSet.clear();
            log.error(ex.getMessage());
            ex.printStackTrace();
            System.exit(2);
        } finally {
        	try {
	            if (rs != null) {
	                    rs.close();
	            }
	            if (stmt != null) {
	                    stmt.close();
	            }
	            if (con != null) {
	                    con.close();
	            }
        	} catch(Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
                System.exit(2);        		
        	}
        }
        log.info("******************** Leaving list method with " + artifactMetadataSet.size() + " ArtifactMetadata objects in it");
        return artifactMetadataSet;
    }

    /**
     * Convert an artifact URI to a storage ID.
     *
     * @param artifactURI
     *            The artifact URI to be converted.
     * @return A string representing the storage ID
     * @throws IllegalArgumentException
     *             If an aspect of the artifact uri is incorrect.
     */
    @Override
    public String toStorageID(String artifactURI) throws IllegalArgumentException {
        log.info("******************** Entering toStorageID method for artifactURI = '" + artifactURI + "'");
        File fileLocation = null;
        
        fileLocation = parsePath(artifactURI);

        log.info("******************** Leaving toStorageID method with storageId = '" + fileLocation.getAbsolutePath() + "'");
        return fileLocation.getAbsolutePath();
    }

    /**
     * Saves the file from the input stream to the final storage location. The MD5 checksum is computed
     * as the stream is readed, and compared with the metadata MD5. 
     * @param artifactURI
     * @param in
     * @param metadataMD5
     * @return True if the MD5 comparation and the file storage has ben performed OK.
     * @throws UnsupportedOperationException
     *             If the checksum algorith is not supported.
     * @throws IllegalArgumentException
     *             If an aspect of the artifact uri is incorrect.
     * @throws TransientException
     *             If an unexpected runtime error occurs.
     */
    private void saveFile(URI artifactURI, InputStream in, String metadataMD5) throws IllegalArgumentException, TransientException{
    	
    	MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new UnsupportedOperationException(e.getMessage(), e);
		}
    	DigestInputStream md5Stream = new DigestInputStream(in, md);
    	
        File file = null;
        log.info("******************** Entering saveFile method for artifactURI = '" + artifactURI + "'");

        file = parsePath(artifactURI.toString());

        boolean toBeCompressed = file.getName().endsWith(".gz");

        File fileDir = file.getParentFile();

        log.info("STARTED saving file '" + file.getAbsolutePath() + "'");

        try {
            fileDir.mkdirs();
        } catch (Exception e) {
            throw new TransientException(e.getMessage(),e);
        }
        
        
        File tempFile = null;
        try {
            tempFile = File.createTempFile(file.getName(), ".tmp", fileDir);
            log.info("Writing temporary file: "+tempFile.getAbsolutePath());
            FileOutputStream fos = new FileOutputStream(tempFile);
        
            if (toBeCompressed) {
                GZIPOutputStream gzipOS = null;
                try {
                   
                    gzipOS = new GZIPOutputStream(fos);

                    int read = 0;
                    byte[] bytes = new byte[2048];

                    while ((read = md5Stream.read(bytes)) != -1) {
                        gzipOS.write(bytes, 0, read);
                    }
                    gzipOS.finish();
                    gzipOS.flush();

                } catch (IOException ex) {
                	log.error("Error saving "+artifactURI+" to "+tempFile.getAbsolutePath()+": "+ex.getMessage());
                    throw new TransientException(ex.getMessage(),ex);
                } finally {
                    if (gzipOS != null) {
                        gzipOS.close();
                    }
                }
            } else {
                try {
                    int read = 0;
                    byte[] bytes = new byte[2048];

                    while ((read = md5Stream.read(bytes)) != -1) {
                        fos.write(bytes, 0, read);
                    }
                    fos.flush();

                } catch (IOException ex) {
                	log.error("Error saving "+artifactURI+" to "+tempFile.getAbsolutePath()+": "+ex.getMessage());
                	throw new TransientException(ex.getMessage(),ex);
                } finally {
                    if (fos != null) {
                        fos.close();
                    }
                }
            }
            
            // Checksum check
            
            byte[] digest = md.digest();
            String computedMD5 = "md5:"+getHexString(digest);
            
            if(!computedMD5.equals(metadataMD5)) {
            	String msg = "Error saving "+artifactURI+". Mismatch between received checksum (" + metadataMD5 + ") and the calculated one (" + computedMD5 + ")";
            	log.error(msg);
            	throw new TransientException(msg);
            }

            
            // Move temp file to final destination
            Files.move(tempFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING );
            
        }catch(IOException e) {
        	log.error(e.getMessage());
        	throw new TransientException(e.getMessage(), e);
        }finally {
        	//Delete temporary file
        	if(tempFile!=null && tempFile.exists()) {
        		tempFile.delete();
        	}
        }
        
        log.info("******************** Leaving saving file '" + file.getAbsolutePath() + "'");

    }

    /**
     * Creates the file path where the artifact is to be stored physically. 
     * If the file is to be compressed before storing, a ".gz" extension is added.
     * @param artifactURI
     * @return
     * @throws IllegalArgumentException
     */
    protected abstract File parsePath(String artifactURI) throws IllegalArgumentException;


    /**
     * Process results from a batch of files downloaded.
     *
     * @param total
     *            Number of files processed.
     * @param successes
     *            Number of files actually downloaded.
     * @param totalElapsedTime
     *            Total elapsed time in ms.
     * @param totalBytes
     *            Total bytes downloaded.
     * @param threads
     *            Threads used.
     */
    @Override
    public void processResults(long total, long successes, long totalElapsedTime, long totalBytes, int threads) {
        String date = dateFormat.format(new Date());
        synchronized (date) {
            try {
                String message = null;
                double figure = 0;
                String units = null;
                if (total > 0) {
                    if (successes > 0) {
                        figure = (totalBytes / ((totalElapsedTime / 1000) * threads));
//                        if (figure >= 1048576.0) { //MB
//                            figure = figure / 1048576.0;
//                            units = "MB/second per thread";
//                            //message += " -> '" + decimalFormat.format(figure) + "' MB/second per thread. " + successes + " files downloaded out of " + total;
//                        } else if (figure >= 1024.0) { // KB
                            figure = figure / 1024.0;
                            units = "KB/second per thread";
//                            //message += " -> '" + decimalFormat.format(figure) + "' KB/second per thread. " + successes + " files downloaded out of " + total;
//                        } else {
//                            units = "B/second per thread";
//                            //message += " -> '" + decimalFormat.format(figure) + "'  B/second per thread. " + successes + " files downloaded out of " + total;
//                        }
                    } else {
                        message = "None of the " + total + " artifacts was able to be downloaded";
                    }
                } else {
                    message = "No artifacts to be download";
                }
                EsacResultsPersistance.getInstance().insert(date, total, successes, totalElapsedTime, totalBytes, threads,
                        Float.parseFloat(decimalFormat.format(figure)), units, message);
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
        if (ConfigProperties.isForceShutdown()) {
        	System.exit(3);
        }
    }
    
    private String getHexString(byte[] bytes) {
    	// convert the byte to hex format method 1
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
        }
        
        return sb.toString();
    }
    
}