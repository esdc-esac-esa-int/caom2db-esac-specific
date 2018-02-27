
package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

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
    private static AtomicInteger id = new AtomicInteger(0);

    private JSONObject jsonProposal = null;

    public Worker(JSONObject jsonProposal) {
        this.jsonProposal = jsonProposal;
    }

    @Override
    public Proposal call() throws Exception {
        Proposal proposal = new Proposal();
        proposal.setId(id.getAndIncrement());
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
        }
        proposal.setNumPublications(bibcodes.size());
        proposal.setNumObservations(0);

        return proposal;
    }
}