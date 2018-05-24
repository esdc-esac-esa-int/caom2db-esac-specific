package esac.archive.ehst.dl.caom2.repo.client.publications.entities;

import static javax.persistence.GenerationType.IDENTITY;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

@Entity
@Table(name = "publication_proposal", catalog = "hst", uniqueConstraints = {@UniqueConstraint(columnNames = "publication_proposal_oid")})
public class PublicationProposal implements java.io.Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 3112207591726870669L;
    private Integer publicationProposalOid;
    private Proposal proposal;
    private Publication publication;

    public PublicationProposal() {

    }

    @Id
    @GeneratedValue(strategy = IDENTITY)
    @Column(name = "publication_proposal_oid", unique = true, nullable = false)
    public Integer getPublicationProposalOid() {
        return publicationProposalOid;
    }
    public void setPublicationProposalOid(Integer publicationProposalOid) {
        this.publicationProposalOid = publicationProposalOid;
    }

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "proposal_oid", nullable = false)
    public Proposal getProposal() {
        return this.proposal;
    }

    public void setProposal(Proposal proposal) {
        this.proposal = proposal;
    }

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "publication_oid", nullable = false)
    public Publication getPublication() {
        return this.publication;
    }

    public void setPublication(Publication publication) {
        this.publication = publication;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof PublicationProposal)) {
            return false;
        }
        PublicationProposal pp = (PublicationProposal) obj;
        return publicationProposalOid.equals(pp.getPublicationProposalOid());
    }
}
