package esac.archive.ehst.dl.caom2.repo.client.publications.entities;

import static javax.persistence.GenerationType.IDENTITY;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

@Entity
@Table(name = "publication", catalog = "hst", uniqueConstraints = {@UniqueConstraint(columnNames = "publication_oid"),
        @UniqueConstraint(columnNames = "bib_code")})
public class Publication implements java.io.Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 6801137300151998329L;
    private Integer publicationOid;
    private String bibcode;
    private String title;
    private String authors;
    private String pubAbstract;
    private String journal;
    private Short year;
    private Integer pageNumber;
    private Short volumeNumber;
    private Integer numberOfObservations = 0;
    private Integer numberOfProposals = 0;

    private Set<PublicationProposal> publicationProposals = new HashSet<PublicationProposal>();

    public Publication() {

    }

    @Id
    @GeneratedValue(strategy = IDENTITY)
    @Column(name = "publication_oid", unique = true, nullable = false)
    public Integer getPublicationOid() {
        return publicationOid;
    }
    public void setPublicationOid(Integer publicationOid) {
        this.publicationOid = publicationOid;
    }

    @Column(name = "bib_code", unique = true, nullable = false)
    public String getBibcode() {
        return bibcode;
    }
    public void setBibcode(String bibcode) {
        this.bibcode = bibcode;
    }

    @Column(name = "title", unique = false)
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }

    @Column(name = "authors", unique = false)
    public String getAuthors() {
        return authors;
    }
    public void setAuthors(String authors) {
        this.authors = authors;
    }

    @Column(name = "abstract", unique = false)
    public String getPubAbstract() {
        return pubAbstract;
    }
    public void setPubAbstract(String pubAbstract) {
        this.pubAbstract = pubAbstract;
    }

    @Column(name = "journal", unique = false)
    public String getJournal() {
        return journal;
    }
    public void setJournal(String journal) {
        this.journal = journal;
    }

    @Column(name = "year", unique = false)
    public Short getYear() {
        return year;
    }
    public void setYear(Short year) {
        this.year = year;
    }

    @Column(name = "page_number", unique = false)
    public Integer getPageNumber() {
        return pageNumber;
    }
    public void setPageNumber(Integer pageNumber) {
        this.pageNumber = pageNumber;
    }

    @Column(name = "volume_number", unique = false)
    public Short getVolumeNumber() {
        return volumeNumber;
    }
    public void setVolumeNumber(Short volumeNumber) {
        this.volumeNumber = volumeNumber;
    }

    @Column(name = "no_observations", unique = false, nullable = false)
    public Integer getNumberOfObservations() {
        return numberOfObservations;
    }
    public void setNumberOfObservations(Integer numberOfObservations) {
        this.numberOfObservations = numberOfObservations;
    }

    @Column(name = "no_proposals", unique = false, nullable = false)
    public Integer getNumberOfProposals() {
        return numberOfProposals;
    }
    public void setNumberOfProposals(Integer numberOfProposals) {
        this.numberOfProposals = numberOfProposals;
    }

    @OneToMany(fetch = FetchType.EAGER, mappedBy = "publication")
    public Set<PublicationProposal> getPublicationProposals() {
        return publicationProposals;
    }

    public void setPublicationProposals(Set<PublicationProposal> publicationProposals) {
        this.publicationProposals = publicationProposals;
    }

    public void addPublicationProposal(PublicationProposal publicationProposal) {
        this.publicationProposals.add(publicationProposal);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Publication)) {
            return false;
        }
        Publication pub = (Publication) obj;
        return bibcode.equals(pub.getBibcode());
    }
}
