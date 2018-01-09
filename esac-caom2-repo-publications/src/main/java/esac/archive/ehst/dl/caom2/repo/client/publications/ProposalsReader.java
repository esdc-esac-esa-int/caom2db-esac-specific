
package esac.archive.ehst.dl.caom2.repo.client.publications;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.reg.client.RegistryClient;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;

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
    private final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    private RegistryClient rc;
    private URI resourceID = null;
    private URL baseServiceURL = null;

    private int nthreads = 1;

    public void init(URI resourceID, int nthreads) {
        this.nthreads = nthreads;
        this.resourceID = resourceID;
        this.rc = new RegistryClient();
    }

    public String read(String url) throws MalformedURLException, IOException {
        return UrlConnectionReader.read(url);
    }
}