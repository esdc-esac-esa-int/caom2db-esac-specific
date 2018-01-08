
package esac.archive.ehst.dl.caom2.repo.client.publications;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessControlException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

public class RepoClient {

    private static final Logger log = Logger.getLogger(RepoClient.class);
    private static final Integer MAX_NUMBER = 3000;

    private final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    private RegistryClient rc;
    private URI resourceID = null;
    private URL baseServiceURL = null;
    private URL baseDeletionURL = null;

    private int nthreads = 1;
    private Comparator<ObservationState> maxLasModifiedComparator = new Comparator<ObservationState>() {
        @Override
        public int compare(ObservationState o1, ObservationState o2) {
            return o1.maxLastModified.compareTo(o2.maxLastModified);
        }
    };

    /**
     * Create new CAOM RepoClient.
     *
     * @param resourceID
     *            the service identifier
     * @param nthreads
     *            number of threads to use when getting list of observations
     */
    public RepoClient(URI resourceID, int nthreads) {
        this.nthreads = nthreads;
        this.resourceID = resourceID;
        this.rc = new RegistryClient();
    }

    private void init() {
        Subject s = AuthenticationUtil.getCurrentSubject();
        AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(s);
        if (meth == null) {
            meth = AuthMethod.ANON;
        }
        this.baseServiceURL = rc.getServiceURL(this.resourceID, Standards.CAOM2REPO_OBS_23, meth);
        if (baseServiceURL == null) {
            throw new RuntimeException("not found: " + resourceID + " + " + Standards.CAOM2REPO_OBS_23 + " + " + meth);
        }
        log.debug("observation list URL: " + baseServiceURL.toString());
        log.debug("AuthMethod:  " + meth);
    }

    private void initDel() {
        Subject s = AuthenticationUtil.getCurrentSubject();
        AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(s);
        if (meth == null) {
            meth = AuthMethod.ANON;
        }
        this.baseDeletionURL = rc.getServiceURL(resourceID, Standards.CAOM2REPO_DEL_23, meth);
        if (baseDeletionURL == null) {
            throw new RuntimeException("not found: " + resourceID + " + " + Standards.CAOM2REPO_DEL_23 + " + " + meth);
        }
        log.debug("deletion list URL: " + baseDeletionURL.toString());
        log.debug("AuthMethod:  " + meth);
    }

    public List<DeletedObservation> getDeleted(String collection, Date start, Date end, Integer maxrec) {
        initDel();

        final List<DeletedObservation> ret = new ArrayList<>();

        // TODO: make call(s) to the deletion endpoint until requested number of entries (like getObservationList)

        // parse each line into the following 4 values, create DeletedObservation, and add to output list, eg:
        /*
         * UUID id = null; String col = null; String observationID = null; Date lastModified = null; DeletedObservation de = new DeletedObservation(id, new
         * ObservationURI(col, observationID)); CaomUtil.assignLastModified(de, lastModified, "lastModified"); ret.add(de);
         */

        return ret;
    }

    public List<ObservationState> getObservationList(String collection, Date start, Date end, Integer maxrec) throws AccessControlException {
        init();

        List<ObservationState> accList = new ArrayList<>();
        List<ObservationState> partialList = null;
        boolean tooBigRequest = maxrec == null || maxrec > MAX_NUMBER;

        Integer rec = maxrec;
        Integer recCounter;
        if (tooBigRequest) {
            rec = MAX_NUMBER;
        }
        // Use HttpDownload to make the http GET calls (because it handles a lot
        // of the
        // authentication stuff)
        boolean go = true;
        String surlCommon = baseServiceURL.toExternalForm() + File.separator + collection;

        while (go) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            if (!tooBigRequest) {
                go = false;// only one go
            }
            String surl = surlCommon;
            surl = surl + "?maxRec=" + (rec + 1);
            if (start != null) {
                surl = surl + "&start=" + df.format(start);
            }
            if (end != null) {
                surl = surl + "&end=" + df.format(end);
            }
            URL url;
            log.debug("URL: " + surl);
            try {
                url = new URL(surl);
                HttpDownload get = new HttpDownload(url, bos);
                get.setFollowRedirects(true);

                get.run();
                int responseCode = get.getResponseCode();
                log.debug("RESPONSE CODE: '" + responseCode + "'");
                /*
                 * if (responseCode == 302) // redirected url { url = get.getRedirectURL(); log.debug("REDIRECTED URL: " + url); bos = new
                 * ByteArrayOutputStream(); get = new HttpDownload(url, bos); responseCode = get.getResponseCode();
                 * log.debug("RESPONSE CODE (REDIRECTED URL): '" + responseCode + "'");
                 * 
                 * }
                 */

                if (get.getThrowable() != null) {
                    if (get.getThrowable() instanceof AccessControlException) {
                        throw (AccessControlException) get.getThrowable();
                    }
                    throw new RuntimeException("failed to get observation list", get.getThrowable());
                }
            } catch (MalformedURLException e) {
                throw new RuntimeException("BUG: failed to generate observation list url", e);
            }

            try {
                // log.debug("RESPONSE = '" + bos.toString() + "'");
                partialList = transformByteArrayOutputStreamIntoListOfObservationState(bos, df, '\t', '\n');
                if (partialList != null && !partialList.isEmpty() && !accList.isEmpty() && accList.get(accList.size() - 1).equals(partialList.get(0))) {
                    partialList.remove(0);
                }

                if (partialList != null) {
                    accList.addAll(partialList);
                    log.debug("adding " + partialList.size() + " elements to accList. Now there are " + accList.size());
                }

                bos.close();
            } catch (ParseException | URISyntaxException | IOException e) {
                throw new RuntimeException("Unable to list of ObservationState from " + bos.toString(), e);
            }

            if (accList.size() > 0) {
                start = accList.get(accList.size() - 1).maxLastModified;
            }

            recCounter = accList.size();
            if (maxrec != null && maxrec - recCounter > 0 && maxrec - recCounter < rec) {
                rec = maxrec - recCounter;
            }
            log.debug("dynamic batch (rec): " + rec);
            log.debug("counter (recCounter): " + recCounter);
            log.debug("maxrec: " + maxrec);
            // log.debug("start: " + start.toString());
            // log.debug("end: " + end.toString());

            if (partialList != null) {
                log.debug("partialList.size(): " + partialList.size());

                if (partialList.size() < rec || (end != null && start != null && start.equals(end))) {
                    log.debug("************** go false");

                    go = false;
                }
            }
        }
        return partialList;
    }

    public List<ObservationResponse> getList(String collection, Date startDate, Date end, Integer numberOfObservations)
            throws InterruptedException, ExecutionException {
        init();

        // startDate = null;
        // end = df.parse("2017-06-20T09:03:15.360");
        List<ObservationResponse> list = new ArrayList<>();

        List<ObservationState> stateList = getObservationList(collection, startDate, end, numberOfObservations);

        // Create tasks for each file
        List<Callable<ObservationResponse>> tasks = new ArrayList<>();

        // the current subject usually gets propagated into a thread pool, but
        // gets attached
        // when the thread is created so we explicitly pass it it and do another
        // Subject.doAs in
        // case
        // thread pool management is changed
        Subject subjectForWorkerThread = AuthenticationUtil.getCurrentSubject();
        for (ObservationState os : stateList) {
            tasks.add(new Worker(os, subjectForWorkerThread, baseServiceURL.toExternalForm()));
        }

        ExecutorService taskExecutor = null;
        try {
            // Run tasks in a fixed thread pool
            taskExecutor = Executors.newFixedThreadPool(nthreads);
            List<Future<ObservationResponse>> futures;

            futures = taskExecutor.invokeAll(tasks);

            for (Future<ObservationResponse> f : futures) {
                ObservationResponse res = null;
                res = f.get();

                if (f.isDone()) {
                    list.add(res);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error when executing thread in ThreadPool: " + e.getMessage() + " caused by: " + e.getCause().toString());
            throw e;
        } finally {
            if (taskExecutor != null) {
                taskExecutor.shutdown();
            }
        }

        return list;
    }

    public ObservationResponse get(ObservationURI uri) {
        init();
        if (uri == null) {
            throw new IllegalArgumentException("uri cannot be null");
        }

        ObservationState os = new ObservationState(uri);

        // see comment above in getList
        Subject subjectForWorkerThread = AuthenticationUtil.getCurrentSubject();
        Worker wt = new Worker(os, subjectForWorkerThread, baseServiceURL.toExternalForm());
        return wt.getObservation();
    }

    public ObservationResponse get(String collection, URI uri, Date start) {
        if (uri == null) {
            throw new IllegalArgumentException("uri cannot be null");
        }

        init();

        log.debug("******************* getObservationList(collection, start, null, null) " + collection);

        List<ObservationState> list = getObservationList(collection, start, null, null);
        ObservationState obsState = null;
        for (ObservationState os : list) {
            if (!os.getURI().getURI().equals(uri)) {
                continue;
            }
            obsState = os;
            break;
        }

        log.debug("******************* getting to getList " + obsState);

        if (obsState != null) {
            // see comment above in getList
            Subject subjectForWorkerThread = AuthenticationUtil.getCurrentSubject();
            Worker wt = new Worker(obsState, subjectForWorkerThread, baseServiceURL.toExternalForm());
            return wt.getObservation();
        } else {
            return null;
        }
    }

    private List<ObservationState> transformByteArrayOutputStreamIntoListOfObservationState(final ByteArrayOutputStream bos, DateFormat sdf, char separator,
            char endOfLine)

            throws ParseException, IOException, URISyntaxException {
        init();

        List<ObservationState> list = new ArrayList<>();

        String id = null;
        String sdate;
        Date date = null;
        String collection = null;
        String md5;
        String aux = "";

        boolean readingDate = false;
        boolean readingCollection = true;
        boolean readingId = false;

        for (int i = 0; i < bos.toString().length(); i++) {
            char c = bos.toString().charAt(i);

            if (c != ' ' && c != separator && c != endOfLine) {
                aux += c;
            } else if (c == separator) {
                if (readingCollection) {
                    collection = aux;
                    // log.debug("*************** collection: " + collection);
                    readingCollection = false;
                    readingId = true;
                    readingDate = false;
                    aux = "";
                } else if (readingId) {
                    id = aux;
                    // log.debug("*************** id: " + id);
                    readingCollection = false;
                    readingId = false;
                    readingDate = true;
                    aux = "";
                } else if (readingDate) {
                    sdate = aux;
                    // log.debug("*************** sdate: " + sdate);
                    date = DateUtil.flexToDate(sdate, sdf);

                    readingCollection = false;
                    readingId = false;
                    readingDate = false;
                    aux = "";
                }

            } else if (c == ' ') {
                if (readingDate) {
                    sdate = aux;
                    // log.debug("*************** sdate: " + sdate);
                    date = DateUtil.flexToDate(sdate, sdf);

                    readingCollection = false;
                    readingId = false;
                    readingDate = false;
                    aux = "";
                }
            } else if (c == endOfLine) {
                if (id == null || collection == null) {
                    continue;
                }

                ObservationState os = new ObservationState(new ObservationURI(collection, id));

                if (date == null) {
                    sdate = aux;
                    date = DateUtil.flexToDate(sdate, sdf);
                }

                os.maxLastModified = date;

                md5 = aux;
                aux = "";
                // log.debug("*************** md5: " + md5);
                if (!md5.equals("")) {
                    os.accMetaChecksum = new URI(md5);
                }

                // if (os.maxLastModified == null)
                // {
                // log.debug("*************** NO DATE");
                // System.exit(1);
                // }
                list.add(os);
                readingCollection = true;
                readingId = false;
            }
        }
        Collections.sort(list, maxLasModifiedComparator);
        return list;

    }
}