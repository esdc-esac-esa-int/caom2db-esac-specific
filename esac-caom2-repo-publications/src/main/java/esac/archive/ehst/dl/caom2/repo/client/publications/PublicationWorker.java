package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.util.concurrent.Callable;

import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Proposal;

public class PublicationWorker implements Callable<Proposal> {

    @Override
    public Proposal call() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
    //    private Proposal proposal = null;
    //    private static final Logger log = Logger.getLogger(PublicationWorker.class);
    //
    //    private static List currentPublications = null;
    //
    //    PublicationWorker(Proposal proposal) {
    //        this.setProposal(proposal);
    //    }
    //    @Override
    //    public Proposal call() throws Exception {
    //        if (getProposal() == null) {
    //            return null;
    //        }
    //        Set<Publication> pubs = readPublications(getProposal().getBibcodes());
    //        getProposal().setPublications(pubs);
    //        return getProposal();
    //    }
    //
    //    private Set<Publication> readPublications(Set<String> bibcodes) throws Exception {
    //        Set<Publication> pubs = new HashSet<Publication>();
    //        BufferedReader in = null;
    //        URL readUrl = null;
    //        HttpURLConnection connection = null;
    //
    //        //        log.info("inside readPublications for bibcodes.size() = " + bibcodes.size());
    //        String bibs = "";
    //        int max = 0;
    //        int accumulated = 0;
    //        int size = bibcodes.size();
    //        for (String b : bibcodes) {
    //            bibs += b + "+or+";
    //            max++;
    //            accumulated++;
    //            if (max == 10 || accumulated == size) { // ADS API limited to 10 bibcodes per batch
    //                String url = "";
    //                try {
    //                    bibs = bibs.substring(0, bibs.length() - 4);
    //                    url = ConfigProperties.getInstance().getAdsUrl().replace("BIBCODE", bibs + "&");
    //                    //                    log.info("inside readPublication with token " + ConfigProperties.getInstance().getAdsToken() + " for url = " + url);
    //
    //                    readUrl = new URL(url);
    //                    connection = (HttpURLConnection) readUrl.openConnection();
    //                    connection.setRequestMethod("GET");
    //                    connection.setRequestProperty("Authorization", "Bearer " + ConfigProperties.getInstance().getAdsToken());
    //                    connection.connect();
    //
    //                    in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    //                    String result = in.readLine();
    //                    //                    log.info("publication read from ADS = " + result);
    //                    List<Publication> processed = processPublications(result);
    //                    if (processed != null && processed.size() > 0) {
    //                        pubs.addAll(processed);
    //                    }
    //                } catch (Exception e) {
    //                    String exceptionMesssage = e.getMessage();
    //                    log.error(exceptionMesssage);
    //                    throw new Exception(exceptionMesssage);
    //                } finally {
    //                    if (in != null) {
    //                        try {
    //                            in.close();
    //                        } catch (IOException e) {
    //                        }
    //                    }
    //                    if (connection != null) {
    //                        connection.disconnect();
    //                    }
    //                    bibs = "";
    //                    max = 0;
    //                }
    //
    //            }
    //        }
    //
    //        return pubs;
    //    }
    //
    //    public Proposal getProposal() {
    //        return proposal;
    //    }
    //    public static void setCurrentPublications(List currentPublications) {
    //        PublicationWorker.currentPublications = currentPublications;
    //    }
    //    public void setProposal(Proposal proposal) {
    //        this.proposal = proposal;
    //    }
    //
}
