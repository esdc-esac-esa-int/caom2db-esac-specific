
package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Proposal;

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
        proposal.setPropId((Long) jsonProposal.get("prop_id"));
        proposal.setTitle((String) jsonProposal.get("title"));
        proposal.setType((String) jsonProposal.get("type"));
        proposal.setCycle((Long) jsonProposal.get("cycle"));
        proposal.setSciCat((String) jsonProposal.get("sci_cat"));
        proposal.setPubAbstract((String) jsonProposal.get("abstract"));
        proposal.setPiName((String) jsonProposal.get("mi") + " " + (String) jsonProposal.get("fname") + " " + (String) jsonProposal.get("lname"));
        JSONArray bibcodes = (JSONArray) jsonProposal.get("bibcode");
        for (Object bibcode : bibcodes) {
            String bib = (String) bibcode;
            proposal.addBibcodes(bib);
        }

        return proposal;
    }
}