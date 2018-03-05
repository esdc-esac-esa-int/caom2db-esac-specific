package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import esac.archive.ehst.dl.caom2.repo.client.publications.db.ConfigProperties;
import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Proposal;
import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Publication;

public class PublicationWorker implements Callable<Proposal> {
    Proposal proposal = null;
    private static final Logger log = Logger.getLogger(PublicationWorker.class);

    PublicationWorker(Proposal proposal) {
        this.proposal = proposal;
    }
    @Override
    public Proposal call() throws Exception {
        if (proposal == null) {
            return null;
        }
        Set<Publication> pubs = readPublications(proposal.getBibcodes());
        proposal.setPublications(pubs);
        return proposal;
    }

    private Set<Publication> readPublications(Set<String> bibcodes) {
        Set<Publication> pubs = new HashSet<Publication>();
        BufferedReader in = null;
        URL readUrl = null;
        HttpURLConnection connection = null;

        //        log.info("inside readPublications for bibcodes.size() = " + bibcodes.size());
        String bibs = "";
        int max = 0;
        int accumulated = 0;
        int size = bibcodes.size();
        for (String b : bibcodes) {
            bibs += b + "+or+";
            max++;
            accumulated++;
            if (max == 10 || accumulated == size) { // ADS API limited to 10 bibcodes per batch
                try {
                    bibs = bibs.substring(0, bibs.length() - 4);
                    String url = ConfigProperties.getInstance().getAdsUrl().replace("BIBCODE", bibs + "&");
                    //                    log.info("inside readPublication with token " + ConfigProperties.getInstance().getAdsToken() + " for url = " + url);

                    readUrl = new URL(url);
                    connection = (HttpURLConnection) readUrl.openConnection();
                    connection.setRequestMethod("GET");
                    connection.setRequestProperty("Authorization", "Bearer " + ConfigProperties.getInstance().getAdsToken());
                    connection.connect();

                    in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    String result = in.readLine();
                    //                    log.info("publication read from ADS = " + result);
                    List<Publication> processed = processPublications(result);
                    if (processed != null && processed.size() > 0) {
                        pubs.addAll(processed);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    pubs = null;
                } finally {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    if (connection != null) {
                        connection.disconnect();
                    }
                    bibs = "";
                    max = 0;
                }

            }
        }

        return pubs;
    }

    private List<Publication> processPublications(String result) {
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
        }
        JSONObject response = (JSONObject) ((JSONObject) object).get("response");
        JSONArray d = ((JSONArray) response.get("docs"));
        //        log.info("inside processPublications for d.size() = " + d.size());
        for (int i = 0; i < d.size(); i++) {
            Publication pub = new Publication();
            JSONObject docs = ((JSONObject) ((JSONArray) response.get("docs")).get(i));
            String pAbstract = (String) docs.get("abstract");
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
            pub.setJournal("");
            pub.setPageNumber(page);
            pub.setVolumeNumber(volume);
            pub.setYear(year);
            pub.setPubAbstract(pAbstract);
            pub.setTitle(title);
            pub.setNumberOfObservations(0);
            pub.setNumberOfPublications(0);
            pubs.add(pub);
        }
        return pubs;
    }
    public Proposal getProposal() {
        return proposal;
    }

}
