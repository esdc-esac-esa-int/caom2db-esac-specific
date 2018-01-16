package esac.archive.ehst.dl.caom2.repo.client.publications.entities;

import static javax.persistence.GenerationType.IDENTITY;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

@Entity
@Table(name = "proposal", catalog = "hst", uniqueConstraints = {@UniqueConstraint(columnNames = "proposal_oid"),
        @UniqueConstraint(columnNames = "proposal_id")})
public class Proposal extends SimpleProposal implements java.io.Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1246362122131245438L;
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