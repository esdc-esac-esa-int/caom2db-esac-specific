
package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

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
        for (Object bibcode : bibcodes) {
            String bib = ((String) bibcode).replace('\u0000', ' ');
            proposal.addBibcodes(bib);
            proposal.addPublication(readPublication(bib));
        }
        proposal.setNumPublications(bibcodes.size());
        proposal.setNumObservations(0);

        return proposal;
    }

    private Publication readPublication(String bibcode) {
        Publication pub = null;
        try {
            String url = ConfigProperties.getInstance().getAdsUrl().replace("BIBCODE", bibcode + "&");

            URL readUrl = new URL(url);
            URLConnection connection = readUrl.openConnection();
            connection.setConnectTimeout(5000);
            connection.setRequestProperty(ConfigProperties.getInstance().getAdsAuth(), ConfigProperties.getInstance().getAdsToken());
            connection.connect();

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String result = in.readLine();
            pub = processPublication(result);
        } catch (Exception e) {
            pub = null;
        }
        return pub;
    }

    private Publication processPublication(String result) {
        Publication pub = new Publication();
        return pub;
    }
}