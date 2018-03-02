
package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import esac.archive.ehst.dl.caom2.repo.client.publications.db.ConfigProperties;
import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Proposal;
import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Publication;

/**
 *
 * @author jduran
 *
 */
public class Worker implements Callable<Proposal> {

    private static final Logger log = Logger.getLogger(Worker.class);

    private JSONObject jsonProposal = null;

    public Worker(JSONObject jsonProposal) {
        this.jsonProposal = jsonProposal;
    }

    @Override
    public Proposal call() throws Exception {
        Proposal proposal = new Proposal();
        //        proposal.setId((Long) jsonProposal.get("prop_id"));
        proposal.setPropId((Long) jsonProposal.get("prop_id"));
        proposal.setTitle(((String) jsonProposal.get("title")).replace('\u0000', ' '));
        proposal.setType(((String) jsonProposal.get("type")).replace('\u0000', ' '));
        proposal.setCycle((Long) jsonProposal.get("cycle"));
        proposal.setSciCat(((String) jsonProposal.get("sci_cat")).replace('\u0000', ' '));
        proposal.setPubAbstract(((String) jsonProposal.get("abstract")).replace('\u0000', ' '));
        proposal.setPiName(((String) jsonProposal.get("mi")).replace('\u0000', ' ') + " " + ((String) jsonProposal.get("fname")).replace('\u0000', ' ') + " "
                + ((String) jsonProposal.get("lname")).replace('\u0000', ' '));
        JSONArray bibcodes = (JSONArray) jsonProposal.get("bibcode");
        log.info("reading bibcodes for proposal " + proposal.getPropId());
        for (Object bibcode : bibcodes) {
            String bib = ((String) bibcode).replace('\u0000', ' ');
            log.info("prop_id = " + proposal.getPropId() + " bibcode: " + bib);

            proposal.addBibcodes(bib);
            Publication pub = readPublication(bib);
            if (pub != null) {
                proposal.addPublication(pub);
            }
        }
        proposal.setNumPublications(bibcodes.size());
        proposal.setNumObservations(0);

        return proposal;
    }

    private Publication readPublication(String bibcode) {
        Publication pub = null;
        BufferedReader in = null;
        URL readUrl = null;
        HttpURLConnection connection = null;

        try {
            log.info("inside readPublication for bibcode = " + bibcode);

            String url = ConfigProperties.getInstance().getAdsUrl().replace("BIBCODE", bibcode + "&");
            log.info("inside readPublication with token " + ConfigProperties.getInstance().getAdsToken() + " for url = " + url);

            readUrl = new URL(url);
            connection = (HttpURLConnection) readUrl.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization", "Bearer " + ConfigProperties.getInstance().getAdsToken());
            //            connection.setRequestProperty(ConfigProperties.getInstance().getAdsAuth(), ConfigProperties.getInstance().getAdsToken());
            connection.connect();

            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String result = in.readLine();
            log.info("publication read from ADS = " + result);
            pub = processPublication(result);
        } catch (Exception e) {
            e.printStackTrace();
            pub = null;
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
        }

        return pub;
    }

    private Publication processPublication(String result) {
        Publication pub = new Publication();
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
        if (d.size() > 0) {
            JSONObject docs = ((JSONObject) ((JSONArray) response.get("docs")).get(0));
            String pAbstract = (String) docs.get("abstract");
            String bibcode = (String) docs.get("bibcode");
            Short volume = null;
            try {
                volume = Short.valueOf((String) docs.get("volume"));
            } catch (Exception e) {
            }
            String p = ((String) ((JSONArray) docs.get("page")).get(0));
            Integer page = null;
            try {
                page = Integer.valueOf(p);
            } catch (Exception e) {
            }
            Short year = null;
            try {
                year = Short.valueOf((String) docs.get("year"));
            } catch (Exception e) {
            }
            JSONArray auths = (JSONArray) docs.get("author");
            String authors = "";
            for (Object author : auths) {
                String auth = ((String) author);
                authors += auth + ", ";
            }
            authors = authors.replaceAll(",$", "");
            JSONArray titleArray = (JSONArray) docs.get("title");
            String title = "";
            for (Object t : titleArray) {
                String ti = ((String) t);
                title += ti + ", ";
            }
            title = title.replaceAll(",$", "");
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
        } else {
            pub = null;
        }
        return pub;
    }
}