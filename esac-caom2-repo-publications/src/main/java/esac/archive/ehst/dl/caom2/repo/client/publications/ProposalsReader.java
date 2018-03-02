
package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.log4j.Logger;

/**
 *
 * @author jduran
 *
 */
public class ProposalsReader {

    private static final Logger log = Logger.getLogger(ProposalsReader.class);

    private static ProposalsReader instance = null;
    public static ProposalsReader getInstance() {
        if (instance == null) {
            instance = new ProposalsReader();
        }
        return instance;
    }

    public String read(String url) throws MalformedURLException, IOException {
        return UrlConnectionReader.read(url);
    }
}