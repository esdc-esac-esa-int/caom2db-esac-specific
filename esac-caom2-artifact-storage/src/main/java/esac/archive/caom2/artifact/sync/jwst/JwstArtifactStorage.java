package esac.archive.caom2.artifact.sync.jwst;


import java.beans.PropertyVetoException;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

import esac.archive.caom2.artifact.sync.ConfigProperties;
import esac.archive.caom2.artifact.sync.EsacAbstractArtifactStorage;

/**
 * ESAC Implementation of the ArtifactStore interface.
 *
 * This class interacts with the ESAC archive data web service to perform the artifact operations defined in ArtifactStore.
 *
 * @author jduran
 */
public class JwstArtifactStorage extends EsacAbstractArtifactStorage {
	protected static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    protected static final DecimalFormat decimalFormat = new DecimalFormat(".##");

    private static final Logger log = Logger.getLogger(JwstArtifactStorage.class.getName());

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

    public JwstArtifactStorage() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException,
            SQLException, PropertyVetoException, NullPointerException {

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

    protected File parsePath(String artifact) throws IllegalArgumentException {
        // modification -> mast:HST/product/x0bi0302t_dgr.fits
        String mast = artifact.split("[:]")[0];
        String rest = artifact.substring(mast.length() + 1);
        String[] parts = rest.split("[/]");
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
            throw new IllegalArgumentException("'" + first + "'" + " is not a valid instrument");
        }
        root = filesLocation.endsWith("/") ? filesLocation.substring(0, filesLocation.length() - 1) : filesLocation;
        String path = root + "/" + first + "/" + second + "/" + third + "/" + name;
        if (toBeCompressed) {
            path += ".gz";
        }
        
        File file = new File(path);
        return file;
    }

}