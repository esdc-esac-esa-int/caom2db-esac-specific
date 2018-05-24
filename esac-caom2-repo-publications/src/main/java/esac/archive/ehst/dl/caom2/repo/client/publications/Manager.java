package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.net.ssl.HttpsURLConnection;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import esac.archive.ehst.dl.caom2.repo.client.publications.db.ConfigProperties;
import esac.archive.ehst.dl.caom2.repo.client.publications.db.JdbcSingleton;
import esac.archive.ehst.dl.caom2.repo.client.publications.db.UnableToCreatePorposalsTables;
import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Proposal;
import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Publication;
import esac.archive.ehst.dl.caom2.repo.client.publications.entities.PublicationProposal;

/**
 *
 * @author jduran
 *
 */
public class Manager {
    private static final Logger log = Logger.getLogger(Manager.class);

    public static void addNewProposals(Session session, List<Proposal> currentProposals, List<Proposal> allProposals) throws Exception {
        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();
            if (allProposals.size() > 0) {
                StringBuilder output = new StringBuilder();
                for (Object o : allProposals) {
                    Proposal p = (Proposal) o;
                    if (!currentProposals.contains(p)) {
                        session.saveOrUpdate(p);
                        log.info("proposal added " + p.getId() + " " + p.getPropId());
                        output.append("proposal added " + p.getId() + " " + p.getPropId() + "\n");
                        for (PublicationProposal pp : p.getPublicationsProposals()) {
                            log.info("----> publication " + pp.getPublication().getPublicationOid() + " " + pp.getPublication().getBibcode());
                            output.append("----> publication " + pp.getPublication().getPublicationOid() + " " + pp.getPublication().getBibcode() + "\n");
                            session.saveOrUpdate(pp.getPublication());
                            session.saveOrUpdate(pp);
                        }
                    }
                }
                try (BufferedWriter bw = new BufferedWriter(new FileWriter("output"))) {
                    bw.write(output.toString());
                } catch (IOException e) {
                    throw e;
                }
            }
        } catch (Exception ex) {
            log.error("Error adding new proposals: " + ex.getMessage());
        } finally {
            if (transaction != null) {
                transaction.commit();
            }
        }
    }

    public static void removeOldProposals(Session session, List<Proposal> currentProposals, List<Proposal> allProposals) throws Exception {
        Transaction transaction = session.beginTransaction();
        List<Proposal> auxProposals = new ArrayList<Proposal>();
        try {
            if (currentProposals.size() > 0) {
                for (Object o : currentProposals) {
                    Proposal p = (Proposal) o;
                    if (!allProposals.contains(p)) {
                        log.info("proposal removed " + p.getPropId());
                        session.remove(p);
                        auxProposals.add(p);
                    }
                }
                for (Proposal p : auxProposals) {
                    currentProposals.remove(p);
                    for (PublicationProposal pp : p.getPublicationsProposals()) {
                        if (pp.getPublication().getPublicationProposals().isEmpty()) {
                            session.remove(pp.getPublication());
                        }
                    }
                }
            }
        } catch (Exception ex) {
            log.error("Error removing old proposals: " + ex.getMessage());
        } finally {
            if (transaction != null) {
                transaction.commit();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static List<Proposal> readCurrentProposals(Session session) {
        boolean correct = true;
        List<Proposal> currentProposals = null;
        //        List<Publication> currentPublications = null;
        try {
            currentProposals = session.createQuery("from Proposal").list();
            for (Object o : currentProposals) {
                Proposal p = (Proposal) o;
                for (PublicationProposal pp : p.getPublicationsProposals()) {
                    Publication pub = pp.getPublication();
                    if (pub != null) {
                        p.addBibcodes(pub.getBibcode());
                    }
                }
            }

            log.info("number of proposals read from DB " + currentProposals.size());
        } catch (Exception e) {
            log.error("Error reading database: " + e.getMessage());
            correct = false;
        }
        if (correct) {
            return currentProposals;
        } else {
            return null;
        }
    }

    public static List<Proposal> readAllProposals(String resource, Integer nThreads) {
        boolean correct = true;
        boolean proposalsChanged = false;

        List<Callable<Proposal>> tasksProposals = new ArrayList<>();
        List<Proposal> newProposals = new ArrayList<Proposal>();
        JSONArray proposals = null;
        String newRead = null;
        String lastRead = null;
        File file = new File("lastRead");
        boolean local = ConfigProperties.getInstance().isLocal();
        boolean noFile = !(file.exists() && !file.isDirectory());
        boolean readFromLocal = local && !noFile;

        try {
            if (readFromLocal) {
                try (BufferedReader br = new BufferedReader(new FileReader("lastRead"))) {
                    lastRead = br.readLine();
                    if (local) {
                        newRead = lastRead;
                    }
                } catch (IOException e) {
                    throw e;
                }
            } else {
                newRead = ProposalsReader.getInstance().read(resource);
            }

            proposalsChanged = !readFromLocal || lastRead == null || !lastRead.equals(newRead);
            lastRead = null;
            System.gc();
            if (proposalsChanged || readFromLocal) {
                if (newRead != null) {
                    if (!readFromLocal) {
                        try (BufferedWriter bw = new BufferedWriter(new FileWriter("lastRead"))) {
                            bw.write(newRead);
                        } catch (IOException e) {
                            throw e;
                        }
                    }
                    JSONParser parser = new JSONParser();
                    Object object = parser.parse(newRead);
                    newRead = null;
                    System.gc();
                    proposals = (JSONArray) object;
                    if (readFromLocal) {
                        log.info("number of proposals found locally = " + proposals.size());
                    } else {
                        log.info("number of proposals found in service = " + proposals.size());
                    }

                    for (Object o : proposals) {
                        tasksProposals.add(new ProposalWorker((JSONObject) o));
                    }
                }
            }

        } catch (ClassCastException | IOException | ParseException e) {
            correct = false;
            log.error("error parsing content from service " + e.getMessage());
            e.printStackTrace();
        }

        if (correct) {
            if (!proposalsChanged && !readFromLocal) {
                log.info("no changes in the content provided by the service since last execution");
                newProposals = null;
            } else {
                ExecutorService taskExecutor = null;
                try {
                    taskExecutor = Executors.newFixedThreadPool(nThreads);
                    List<Future<Proposal>> futures;

                    futures = taskExecutor.invokeAll(tasksProposals);

                    for (Future<Proposal> f : futures) {
                        newProposals.add(f.get());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error when executing thread in ThreadPool: " + e.getMessage() + " caused by: " + e.getCause().toString());
                    correct = false;
                } finally {
                    if (taskExecutor != null) {
                        taskExecutor.shutdown();
                    }
                }
            }
        }
        return newProposals;
    }

    public static boolean tablesExist() throws SQLException, PropertyVetoException, UnableToCreatePorposalsTables {
        boolean correct = true;
        String queryProposals = "select exists (select 1 from information_schema.tables where table_schema = '" + ConfigProperties.getInstance().getSchema()
                + "' and table_name = 'proposal')";
        String queryPublications = "select exists (select 1 from information_schema.tables where table_schema = '" + ConfigProperties.getInstance().getSchema()
                + "' and table_name = 'publication')";
        String queryPropPub = "select exists (select 1 from information_schema.tables where table_schema = '" + ConfigProperties.getInstance().getSchema()
                + "' and table_name = 'publication_proposal')";
        //        log.debug("queryProposals = " + queryProposals);
        //        log.debug("queryPublications = " + queryPublications);
        //        log.debug("queryPropPub = " + queryPropPub);
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        boolean existsProposals = false;
        boolean existsPublications = false;
        boolean existsPropPub = false;

        try {
            con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            rs = stmt.executeQuery(queryProposals);
            if (rs.next()) {
                existsProposals = rs.getBoolean(1);
            }
            log.debug(ConfigProperties.getInstance().getSchema() + ".proposal exists = " + existsProposals);
            rs.close();
            if (existsProposals) {
                rs = stmt.executeQuery(queryPublications);
                if (rs.next()) {
                    existsPublications = rs.getBoolean(1);
                }
                log.debug(ConfigProperties.getInstance().getSchema() + ".publication exists = " + existsPublications);
                rs.close();
                if (existsPublications) {
                    rs = stmt.executeQuery(queryPropPub);
                    if (rs.next()) {
                        existsPropPub = rs.getBoolean(1);
                    }
                    log.debug(ConfigProperties.getInstance().getSchema() + ".publication_proposal exists = " + existsPropPub);
                }
            }

        } catch (Exception ex) {
            throw new UnableToCreatePorposalsTables("Unexpected exception when checking proposals and publications table exist: " + ex.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                }
            }
        }
        correct = existsProposals && existsPublications && existsPropPub;
        return correct;
    }

    public static Map<String, Publication> readAllPublications(List<String> bibcodes, String adsUrl, String adsParams, String adsToken) {
        Map<String, Publication> pubs = new HashMap<String, Publication>();
        List<Publication> processed = new ArrayList<Publication>();
        String result = null;
        BufferedReader in = null;
        URL readUrl = null;
        HttpsURLConnection connection = null;

        String bibs = "bibcode";
        int max = 2000;
        int accumulated = 0;
        List<String> currentBibcodes = new ArrayList<String>();
        for (String b : bibcodes) {
            if (currentBibcodes.contains(b)) {
                continue;
            }
            currentBibcodes.add(b);
        }
        int size = currentBibcodes.size();
        boolean first = true;
        for (String b : currentBibcodes) {
            bibs += "\n" + b;
            accumulated++;
            if (accumulated == max || accumulated == size) { // ADS big API limited to 2000 bibcodes per batch
                try {
                    if (first) {
                        first = false;
                        adsUrl += "?";
                        Map<String, Object> params = new LinkedHashMap<>();
                        params.put("q", "*:*");
                        params.put("wt", "json");
                        params.put("fl", "*");
                        params.put("rows", accumulated);

                        StringBuilder postData = new StringBuilder();
                        for (Map.Entry<String, Object> param : params.entrySet()) {
                            if (postData.length() != 0)
                                postData.append('&');
                            postData.append(param.getKey());
                            postData.append('=');
                            postData.append(param.getValue());
                        }

                        adsUrl += postData;
                    }
                    readUrl = new URL(adsUrl);
                    connection = (HttpsURLConnection) readUrl.openConnection();

                    connection.setRequestMethod("POST");
                    connection.setRequestProperty("Authorization", "Bearer " + adsToken);
                    connection.setRequestProperty("User-Agent", "Java client");
                    connection.setRequestProperty("Content-Type", "big-query/csv");

                    connection.setDoOutput(true);
                    try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
                        wr.writeBytes(bibs);
                    }

                    in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    result = in.readLine();
                    //                    try (BufferedWriter bw = new BufferedWriter(new FileWriter("test.json"))) {
                    //                        bw.write(result);
                    //                    } catch (IOException e) {
                    //                        throw e;
                    //                    }
                    processed = processPublications(result);
                } catch (Exception e) {
                    String exceptionMesssage = e.getMessage();
                    log.error(exceptionMesssage);
                    pubs = null;
                } finally {
                    result = null;
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException e) {
                        }
                    }
                    if (connection != null) {
                        connection.disconnect();
                    }
                    bibs = "bibcode";
                    accumulated = 0;
                }
                if (pubs == null) {
                    break;
                }
                if (processed != null && processed.size() > 0) {
                    for (Publication pub : processed) {
                        pubs.put(pub.getBibcode(), pub);
                    }
                }

            }
        }
        currentBibcodes.clear();
        return pubs;
    }

    private static List<Publication> processPublications(String result) {
        List<Publication> pubs = new ArrayList<Publication>();
        //        {"responseHeader":
        //        {"status":0,"QTime":4,"params":{"q":"bibcode:2004ApJ...613..129W","fl":"bibcode,title,author,abstract,year,page,volume","wt":"json"
        //        }
        //        },"
        //        response":
        //        {"numFound":1,"start":0,"docs":[
        //        {"abstract":"We present the results of a search for variability in the equivalent widths (EWs) of narrow, asso..."
        //        ,"year":"2004"
        //        ,"page":["129"]
        //        ,"bibcode":"2004ApJ...613..129W"
        //        ,"author":["Wise, John H.","Eracleous, Michael","Charlton, Jane C.","Ganguly, Rajib"]
        //        ,"volume":"613"
        //        ,"title":["Variability of Narrow, Associated Absorption Lines in Moderate- and Low-Redshift Quasars"]}]}}

        JSONParser parser = new JSONParser();
        Object object = null;
        try {
            object = parser.parse(result);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        } finally {

            result = null;
        }
        JSONObject response = (JSONObject) ((JSONObject) object).get("response");
        JSONArray d = ((JSONArray) response.get("docs"));
        //        log.info("inside processPublications for d.size() = " + d.size());
        for (int i = 0; i < d.size(); i++) {
            Publication pub = new Publication();
            JSONObject docs = ((JSONObject) ((JSONArray) response.get("docs")).get(i));
            String pAbstract = (String) docs.get("abstract");
            String journal = (String) docs.get("pub");
            String bibcode = (String) docs.get("bibcode");
            Short volume = null;
            try {
                volume = Short.valueOf((String) docs.get("volume"));
            } catch (Exception e) {
                volume = 0;
            }
            Integer page = null;
            try {
                String p = ((String) ((JSONArray) docs.get("page")).get(0));
                page = Integer.valueOf(p);
            } catch (Exception e) {
                page = 0;
            }
            Short year = null;
            try {
                year = Short.valueOf((String) docs.get("year"));
            } catch (Exception e) {
                year = 0;
            }
            JSONArray auths = (JSONArray) docs.get("author");
            String authors = "";
            if (auths != null && auths.size() > 0) {
                for (Object author : auths) {
                    String auth = ((String) author);
                    authors += auth + ", ";
                }
                authors = authors.replaceAll(",$", "");
            }
            JSONArray titleArray = (JSONArray) docs.get("title");
            String title = "";
            if (titleArray != null && titleArray.size() > 0) {
                for (Object t : titleArray) {
                    String ti = ((String) t);
                    title += ti + ", ";
                }
                title = title.replaceAll(",$", "");
            }
            pub.setAuthors(authors);
            pub.setBibcode(bibcode);
            pub.setJournal(journal);
            pub.setPageNumber(page);
            pub.setVolumeNumber(volume);
            pub.setYear(year);
            pub.setPubAbstract(pAbstract);
            pub.setTitle(title);
            pub.setNumberOfObservations(0);
            pub.setNumberOfProposals(0);
            pubs.add(pub);
        }
        return pubs;
    }

    public static List<String> getAllBibcodes(List<Proposal> allProposals) {
        List<String> bibcodes = new ArrayList<String>();
        for (Proposal prop : allProposals) {
            bibcodes.addAll(prop.getBibcodes());
        }
        return bibcodes;
    }

    public static List<Proposal> fillPublicationsIntoProposals(List<Proposal> allProposals, Map<String, Publication> allPublications) {
        if (allPublications == null) {
            return null;
        }
        for (Proposal prop : allProposals) {
            for (String bibcode : prop.getBibcodes()) {
                if (allPublications.containsKey(bibcode)) {
                    PublicationProposal pp = new PublicationProposal();
                    Publication pub = allPublications.get(bibcode);
                    pp.setProposal(prop);
                    pp.setPublication(pub);
                    prop.addPublicationProposal(pp);
                    pub.addPublicationProposal(pp);
                    pub.setNumberOfProposals(pub.getNumberOfProposals() + 1);
                }
            }
        }
        return allProposals;
    }

    public static void fool(Session session) {
        try {
            session.getTransaction().begin();
            Proposal p = new Proposal();
            p.setPropId(1000L);
            p.setTitle("Hola");
            p.setCycle(23L);
            p.setNumPublications(1);
            p.setNumObservations(0);
            p.setPubAbstract("Hola y adios");
            p.setSciCat("");
            p.setType("");
            p.setPiName("");
            session.saveOrUpdate(p);
            //            session.getTransaction().commit();

            Publication pub = new Publication();
            pub.setBibcode("2018..23..JDA");
            pub.setAuthors("Yo");
            pub.setJournal("");
            pub.setNumberOfObservations(0);
            pub.setPageNumber(8);
            pub.setNumberOfProposals(7);
            pub.setTitle("");
            pub.setPubAbstract("");
            pub.setVolumeNumber((short) 89);
            session.saveOrUpdate(pub);

            PublicationProposal pp = new PublicationProposal();
            pp.setPublication(pub);
            pp.setProposal(p);

            p.addPublicationProposal(pp);
            pub.addPublicationProposal(pp);

            session.saveOrUpdate(pp);
        } catch (Exception ex) {
            log.error(ex.getMessage());
        } finally {
            session.getTransaction().commit();
        }
        System.exit(1);

    }

}
