package esac.archive.ehst.dl.caom2.repo.client.publications.entities;

import static javax.persistence.GenerationType.IDENTITY;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

@Entity
@Table(name = "publication", catalog = "hst", uniqueConstraints = {@UniqueConstraint(columnNames = "publication_oid"),
        @UniqueConstraint(columnNames = "bib_code")})
public class Publication {
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

    private List<Proposal> proposals = new ArrayList<Proposal>();

    public Publication() {

    }

    @Column(name = "bib_code", unique = true, nullable = false)
    public String getBibcode() {
        return bibcode;
    }
    public void setBibcode(String bibcode) {
        this.bibcode = bibcode;
    }

    @Column(name = "title", unique = false, nullable = false)
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }

    @Column(name = "authors", unique = false, nullable = false)
    public String getAuthors() {
        return authors;
    }
    public void setAuthors(String authors) {
        this.authors = authors;
    }

    @Column(name = "abstract", unique = false, nullable = false)
    public String getPubAbstract() {
        return pubAbstract;
    }
    public void setPubAbstract(String pubAbstract) {
        this.pubAbstract = pubAbstract;
    }

    @Column(name = "journal", unique = false, nullable = false)
    public String getJournal() {
        return journal;
    }
    public void setJournal(String journal) {
        this.journal = journal;
    }

    @Column(name = "year", unique = false, nullable = false)
    public Short getYear() {
        return year;
    }
    public void setYear(Short year) {
        this.year = year;
    }

    @Column(name = "page_number", unique = false, nullable = false)
    public Integer getPageNumber() {
        return pageNumber;
    }
    public void setPageNumber(Integer pageNumber) {
        this.pageNumber = pageNumber;
    }

    @Column(name = "page_number", unique = false, nullable = false)
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

    @Column(name = "no_publications", unique = false, nullable = false)
    public Integer getNumberOfPublications() {
        return numberOfProposals;
    }
    public void setNumberOfPublications(Integer numberOfProposals) {
        this.numberOfProposals = numberOfProposals;
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

    @ManyToMany(fetch = FetchType.LAZY, mappedBy = "publication")
    public List<Proposal> getProposals() {
        return proposals;
    }

    public void setProposals(List<Proposal> proposals) {
        this.proposals = proposals;
    }
}
