package esac.archive.ehst.dl.caom2.repo.client.publications.entities;

import java.util.HashSet;
import java.util.Set;

public class SimpleProposal {
    private Set<String> bibcodes = new HashSet<String>();

    public Set<String> getBibcodes() {
        return bibcodes;
    }

    public void addBibcodes(String bibcode) {
        this.bibcodes.add(bibcode);
    }
}
