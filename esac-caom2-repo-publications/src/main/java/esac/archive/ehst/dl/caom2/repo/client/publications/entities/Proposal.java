package esac.archive.ehst.dl.caom2.repo.client.publications.entities;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.apache.log4j.Logger;

@Entity
@Table(name = "proposal", catalog = "hst", uniqueConstraints = {@UniqueConstraint(columnNames = "proposal_oid"),
        @UniqueConstraint(columnNames = "proposal_id")})
public class Proposal extends SimpleProposal implements java.io.Serializable, Comparable<Proposal> {
    private static final Logger log = Logger.getLogger(Proposal.class);

    /**
     *
     */
    private static final long serialVersionUID = 1246362122131245438L;
    private Long id;
    private Long propId;
    private String piName;
    private String title;
    private String type;
    private Long cycle;
    private String sciCat;
    private String pubAbstract;
    private Integer numPublications;
    private Integer numObservations;

    private Set<Publication> publications = new HashSet<Publication>();

    public Proposal() {
    }

    @Column(name = "proposal_id", unique = true, nullable = false)
    public Long getPropId() {
        return propId;
    }
    public void setPropId(Long propId) {
        this.propId = propId;
    }

    @Column(name = "pi_name", unique = false, nullable = false)
    public String getPiName() {
        return piName;
    }
    public void setPiName(String piName) {
        this.piName = piName;
    }

    @Column(name = "title", unique = false, nullable = false)
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }

    @Column(name = "proposal_type", unique = false, nullable = false)
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }

    @Column(name = "cycle", unique = false, nullable = false)
    public Long getCycle() {
        return cycle;
    }
    public void setCycle(Long cycle) {
        this.cycle = cycle;
    }

    @Column(name = "science_category", unique = false, nullable = false)
    public String getSciCat() {
        return sciCat;
    }
    public void setSciCat(String sciCat) {
        this.sciCat = sciCat;
    }

    @Column(name = "abstract", unique = false, nullable = false)
    public String getPubAbstract() {
        return pubAbstract;
    }
    public void setPubAbstract(String pubAbstract) {
        this.pubAbstract = pubAbstract;
    }

    @ManyToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinTable(name = "publication_proposal", catalog = "hst", joinColumns = {
            @JoinColumn(name = "proposal_oid", nullable = false, updatable = true)}, inverseJoinColumns = {
                    @JoinColumn(name = "publication_oid", nullable = false, updatable = true)})
    public Set<Publication> getPublications() {
        return publications;
    }

    public void setPublications(Set<Publication> publications) {
        this.publications = publications;
    }

    public void addPublication(Publication publication) {
        this.publications.add(publication);
    }

    public void addPublications(List<Publication> pubs) {
        this.publications.addAll(pubs);
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "proposal_oid", unique = true, nullable = false)
    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }

    @Column(name = "no_publications", unique = false, nullable = false)
    public Integer getNumPublications() {
        return numPublications;
    }
    public void setNumPublications(Integer numPublications) {
        this.numPublications = numPublications;
    }

    @Column(name = "no_observations", unique = false, nullable = false)
    public Integer getNumObservations() {
        return numObservations;
    }
    public void setNumObservations(Integer numObservations) {
        this.numObservations = numObservations;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Proposal)) {
            return false;
        }
        Proposal p = (Proposal) obj;
        //        if (p.getPropId() == 1019 || this.getPropId() == 1019) {
        //            log.info("this.getPropId().equals(p.getPropId()) " + this.getPropId().equals(p.getPropId()));
        //            log.info("this.getCycle().equals(p.getCycle()) " + this.getCycle().equals(p.getCycle()));
        //            log.info("this.getNumPublications().equals(p.getNumPublications()) " + this.getNumPublications().equals(p.getNumPublications()));
        //            log.info("this.getPubAbstract().equals(p.getPubAbstract()) " + this.getPubAbstract().equals(p.getPubAbstract()));
        //            log.info("this.getSciCat().equals(p.getSciCat()) " + this.getSciCat().equals(p.getSciCat()));
        //            log.info("this.getTitle().equals(p.getTitle()) " + this.getTitle().equals(p.getTitle()));
        //            log.info("this.getType().equals(p.getType()) " + this.getType().equals(p.getType()));
        //            log.info("this.getNumPublications().equals(p.getNumPublications()) " + this.getNumPublications().equals(p.getNumPublications()));
        //            log.info("this.getBibcodes().size() == p.getBibcodes().size() " + (this.getBibcodes().size() == p.getBibcodes().size()));
        //        }
        return this.getPropId().equals(p.getPropId());
        //                && this.getCycle().equals(p.getCycle()) && this.getNumObservations().equals(p.getNumObservations())
        //                && this.getNumPublications().equals(p.getNumPublications()) && this.getPiName().equals(p.getPiName())
        //                && this.getPubAbstract().equals(p.getPubAbstract()) && this.getSciCat().equals(p.getSciCat()) && this.getTitle().equals(p.getTitle())
        //                && this.getType().equals(p.getType()) && this.getNumPublications().equals(p.getNumPublications())
        //                && this.getBibcodes().size() == p.getBibcodes().size();
    }

    private boolean samePublications(Proposal p1, Proposal p2) {
        boolean same = true;
        for (String bib1 : p1.getBibcodes()) {
            log.info("bib1 " + bib1);
            if (p2.getBibcodes().contains(bib1)) {
                continue;
            }
            same = false;
            break;
        }
        if (same) {
            for (String bib2 : p2.getBibcodes()) {
                log.info("bib2 " + bib2);
                if (p1.getBibcodes().contains(bib2)) {
                    continue;
                }
                same = false;
                break;
            }
        }
        return same;
    }

    @Override
    public int compareTo(Proposal o) {
        if (o == null) {
            return -1;
        } else if (o.getPropId() == null || this.getPropId() == null) {
            return -1;
        }
        return this.getPropId().compareTo(o.getPropId());
    }
}