package esac.archive.ehst.dl.caom2.repo.client.publications.entities;

import static javax.persistence.GenerationType.IDENTITY;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

@Table(name = "proposal", catalog = "hst", uniqueConstraints = {@UniqueConstraint(columnNames = "proposal_oid"),
        @UniqueConstraint(columnNames = "proposal_id")})
public class Proposal {

    @Id
    private Integer id;
    private Long propId;
    private String piName;
    private String title;
    private String type;
    private Long cycle;
    private String sciCat;
    private String pubAbstract;
    private Integer numPublications;
    private Integer numObservations;

    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinTable(name = "publication_proposal", catalog = "hst", joinColumns = {
            @JoinColumn(name = "proposal_oid", nullable = false, updatable = true)}, inverseJoinColumns = {
                    @JoinColumn(name = "publication_oid", nullable = false, updatable = true)})
    private List<String> bibcodes = new ArrayList<String>();

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

    @OneToMany(fetch = FetchType.EAGER, mappedBy = "proposal")
    public List<String> getBibcodes() {
        return bibcodes;
    }

    public void setBibcodes(List<String> bibcodes) {
        this.bibcodes = bibcodes;
    }

    public void addBibcode(String bibcode) {
        this.bibcodes.add(bibcode);
    }

    @Id
    @GeneratedValue(strategy = IDENTITY)
    @Column(name = "proposal_oid", unique = true, nullable = false)
    public Integer getId() {
        return id;
    }
    public void setId(Integer id) {
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
}