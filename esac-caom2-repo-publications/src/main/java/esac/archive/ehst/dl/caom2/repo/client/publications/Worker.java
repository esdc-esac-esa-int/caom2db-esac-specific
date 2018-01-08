
package esac.archive.ehst.dl.caom2.repo.client.publications;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.xml.ObservationParsingException;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.net.HttpDownload;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

public class Worker implements Callable<ObservationResponse> {

    private static final Logger log = Logger.getLogger(Worker.class);

    private ObservationState state = null;
    private Subject subject = null;
    private String baseHTTPURL = null;

    public Worker(ObservationState state, Subject subject, String url) {
        this.state = state;
        this.subject = subject;
        this.baseHTTPURL = url;
    }

    @Override
    public ObservationResponse call() throws Exception {
        return getObservation();
    }

    public ObservationResponse getObservation() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        String surl = baseHTTPURL + File.separator + state.getURI().getURI().getSchemeSpecificPart();
        final URL url;
        try {
            url = new URL(surl);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Unable to create URL object for " + surl);
        }
        HttpDownload get = new HttpDownload(url, bos);

        if (subject != null) {
            Subject.doAs(subject, new RunnableAction(get));

        } else {
            get.run();
        }

        // TODO: need to check get.getResponseCode() and get.getThrowable() for any failure to get the document
        // specifically: 404 if the observation does not/no longer exists is important to distinguish and handle
        ObservationReader obsReader = new ObservationReader();
        ObservationResponse wr = new ObservationResponse(state);

        try {
            // log.info("********************* bos:" + bos.toString());
            wr.observation = obsReader.read(bos.toString());
        } catch (Exception e) {
            String oid = state.getURI().getObservationID();
            wr.error = new IllegalStateException("Unable to create Observation object for id " + oid + ": " + e.getMessage());
        }
        return wr;
    }

    public ObservationResponse getObservation(URI uri) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        String surl = baseHTTPURL + File.separator + state.getURI().getURI().getSchemeSpecificPart();
        log.info("URL: " + surl);
        URL url = null;
        try {
            url = new URL(surl);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Unable to create URL object for " + surl);
        }
        HttpDownload get = new HttpDownload(url, bos);

        if (subject != null) {
            Subject.doAs(subject, new RunnableAction(get));

        } else {
            get.run();
        }

        ObservationReader obsReader = new ObservationReader();
        ObservationResponse wr = new ObservationResponse(state);

        try {
            wr.observation = obsReader.read(bos.toString());
        } catch (ObservationParsingException e) {
            String oid = state.getURI().getObservationID();
            wr.error = new IllegalStateException("Unable to create Observation object for id " + oid + ": " + e.getMessage());
        }
        return wr;
    }

}