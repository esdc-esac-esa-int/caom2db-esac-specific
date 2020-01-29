package esac.archive.ehst.dl.caom2.repo.client.publications;

import ca.nrc.cadc.util.Log4jInit;

import java.beans.PropertyVetoException;
import java.io.File;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.springframework.transaction.annotation.Transactional;

import esac.archive.ehst.dl.caom2.repo.client.publications.db.ConfigProperties;
import esac.archive.ehst.dl.caom2.repo.client.publications.db.UnableToCreatePorposalsTables;
import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Proposal;
import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Publication;

/**
 *
 * @author jduran
 *
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);
    private static Configuration configuration = null;
    private static SessionFactory factory = null;

    @Transactional(rollbackFor = Exception.class)
    public static void main(String[] args) {
        boolean correct = true;
        ArgumentMap am = new ArgumentMap(args);
        if (readConfig(am)) {
            try {
                correct = Manager.tablesExist();
            } catch (SQLException | PropertyVetoException | UnableToCreatePorposalsTables e) {
                log.error("Error when checking existency of the tables or creating them: " + e.getMessage() + " caused by: " + e.getCause().toString());
                correct = false;
            }
            if (correct) {
                log.info("DB tables exist");

                SessionFactory factory = ConfigProperties.getInstance().getFactory();
                Session session = factory.openSession();

                String resource = ConfigProperties.getInstance().getResource();
                Integer threads = ConfigProperties.getInstance().getnThreads();
                String adsUrl = ConfigProperties.getInstance().getAdsUrl();
                String adsToken = ConfigProperties.getInstance().getAdsToken();

                List<Proposal> currentProposals = Manager.readCurrentProposals(session);
                List<Proposal> allProposals = Manager.readAllProposals(resource, threads);
                if (allProposals != null) {
	                List<String> allBibcodes = Manager.getAllBibcodes(allProposals);
	                Map<String, Publication> allPublications = Manager.readAllPublications(allBibcodes, adsUrl, adsToken);
	
	                allProposals = Manager.fillPublicationsIntoProposals(allProposals, allPublications);
	
	                if (allProposals != null && allPublications != null) {
	                    if (currentProposals != null) {
	                        log.info("current proposals " + currentProposals.size());
	                        log.info("all proposals     " + allProposals.size());
	                        try {
	                        	Manager.empty_tables();
	                            log.info("DB tables empty");
//	                            log.info("removing old proposals and publications");
//	                            Manager.removeOldProposals(session, currentProposals, allProposals);
	                            log.info("adding new proposals and publications");
	                            Manager.addNewProposals(session, currentProposals, allProposals);
	                        } catch (Exception ex) {
	                            log.error(ex.getMessage());
	                            correct = false;
	                        }
	                    }
	                }
                }
            	
            }
        }

        System.exit(0);
    }

	private static boolean readConfig(ArgumentMap am) {
        boolean correct = true;
        if (am.isSet("d") || am.isSet("debug")) {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.repo.client.publications", Level.DEBUG);
        } else if (am.isSet("v") || am.isSet("verbose")) {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.repo.client.publications", Level.INFO);
        } else {
            Log4jInit.setLevel("esac.archive.ehst.dl.caom2.repo.client.publications", Level.WARN);
        }
        if (am.isSet("h") || am.isSet("help")) {
            usage();
            System.exit(0);
        }
        if (!am.isSet("hibernate") || !am.isSet("nthreads") || !am.isSet("password")) {
            usage();
            correct = false;
        }
        if (!correct) {
            System.exit(1);
        }

        String hibConfigFile = am.getValue("hibernate");
        System.out.println("hibConfigFile = " + hibConfigFile);
        
        File hib = new File(hibConfigFile);
        boolean isLocal = am.isSet("local");

        configuration = new Configuration().configure(hib);

        Integer nthreads = null;
        try {
            nthreads = Integer.parseInt(am.getValue("nthreads"));
        } catch (NumberFormatException nfe) {
            usage();
        }

        String resource = configuration.getProperty("resource");
        System.out.println("resource = " + resource);
        String driver = configuration.getProperty("hibernate.connection.driver_class");
        System.out.println("driver = " + driver);
        String database = configuration.getProperty("hibernate.connection.database");
        String schema = configuration.getProperty("hibernate.default_schema");
        String host = configuration.getProperty("hibernate.connection.host");
        Integer port = null;
        try {
            port = Integer.parseInt(configuration.getProperty("hibernate.connection.port"));
        } catch (Exception e) {
            usage();
            correct = false;
        }

        String username = configuration.getProperty("hibernate.connection.username");
        //String password = configuration.getProperty("hibernate.connection.password");
        String password = am.getValue("password");
        configuration.setProperty("hibernate.connection.password", password);
        String adsToken = configuration.getProperty("ads.token");
        String adsUrl = configuration.getProperty("ads.url");
        String adsParams = configuration.getProperty("ads.params");
        String obsUpdate = configuration.getProperty("obs.update");

        String connection = "jdbc:postgresql://" + host;
        configuration.setProperty("hibernate.connection.url", "jdbc:postgresql://" + host + ":" + port + "/" + database);

        factory = configuration.buildSessionFactory();

        ConfigProperties.getInstance().init(connection, driver, database, schema, host, port, username, password, adsUrl, adsToken, resource, nthreads, factory,
                isLocal, obsUpdate);

        log.info("config initiated");
        return correct;
    }

    private static Comparator<Proposal> proposalComparator = new Comparator<Proposal>() {
        @Override
        public int compare(Proposal p1, Proposal p2) {
            return p1.getPropId().compareTo(p2.getPropId());
        }
    };

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: esac-caom2-repo-publications [-v|--verbose|-d|--debug] [-h|--help] ...");
        sb.append("\n         --hibernate=path to the hibernate.cfg.xml file");
        sb.append("\n         --password=db password to be used");
        sb.append("\n         --threads=number of threads used to read papers");
        sb.append("\n         --local: use local file containing the set of proposals instead of the response of STScI service");
        sb.append("\n         --maxConnectionsToADS: maxConnectionsToADS per day");
        log.warn(sb.toString());
    }

}
