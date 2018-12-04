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
 * ESAC Implementation of the ArtifactStore interface for JWST archive.
 *
 * This class interacts with the ESAC archive data web service to perform the artifact operations defined in ArtifactStore.
 *
 * @author Raul Gutierrez-Sanchez Copyright (c) 2014- European Space Agency
 */
public class JwstArtifactStorage extends EsacAbstractArtifactStorage {

	private static final Logger log = Logger.getLogger(JwstArtifactStorage.class.getName());

    public JwstArtifactStorage() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException,
            SQLException, PropertyVetoException, NullPointerException {
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

        String root = ConfigProperties.getInstance().getProperty(ConfigProperties.PROP_REPOSITORY_ROOT);
        root = root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
        String path = root + "/" + first + "/" + second + "/" + third + "/" + name;
        if (toBeCompressed) {
            path += ".gz";
        }
        
        File file = new File(path);
        return file;
    }

}