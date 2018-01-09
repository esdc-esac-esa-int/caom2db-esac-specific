package esac.archive.ehst.dl.caom2.repo.client.publications.entities;

import java.util.ArrayList;
import java.util.List;

public class Proposal {
    private Long propId;
    private String fname;
    private String mi;
    private String lname;
    private String title;
    private Integer cycle;
    private String sciCat;
    private String pubAbstract;
    private List<String> bibcodes = new ArrayList<String>();

    public Long getPropId() {
        return propId;
    }
    public void setPropId(Long propId) {
        this.propId = propId;
    }
    public String getFname() {
        return fname;
    }
    public void setFname(String fname) {
        this.fname = fname;
    }
    public String getMi() {
        return mi;
    }
    public void setMi(String mi) {
        this.mi = mi;
    }
    public String getLname() {
        return lname;
    }
    public void setLname(String lname) {
        this.lname = lname;
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    public Integer getCycle() {
        return cycle;
    }
    public void setCycle(Integer cycle) {
        this.cycle = cycle;
    }
    public String getSciCat() {
        return sciCat;
    }
    public void setSciCat(String sciCat) {
        this.sciCat = sciCat;
    }
    public String getPubAbstract() {
        return pubAbstract;
    }
    public void setPubAbstract(String pubAbstract) {
        this.pubAbstract = pubAbstract;
    }
    public List<String> getBibcodes() {
        return bibcodes;
    }
    public void addBibcode(String bibcode) {
        this.bibcodes.add(bibcode);
    }
}