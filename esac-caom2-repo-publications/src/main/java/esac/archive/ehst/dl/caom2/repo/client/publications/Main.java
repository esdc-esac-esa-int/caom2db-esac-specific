package esac.archive.ehst.dl.caom2.repo.client.publications;

import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.transaction.annotation.Transactional;

import esac.archive.ehst.dl.caom2.repo.client.publications.db.ConfigProperties;
import esac.archive.ehst.dl.caom2.repo.client.publications.db.JdbcSingleton;
import esac.archive.ehst.dl.caom2.repo.client.publications.db.UnableToCreatePorposalsTables;
import esac.archive.ehst.dl.caom2.repo.client.publications.entities.Proposal;

/**
 *
 * @author jduran
 *
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);
    private static Configuration configuration = null;
    private static SessionFactory factory = null;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Transactional(rollbackFor = Exception.class)
    public static void main(String[] args) {
        boolean correct = true;

        ArgumentMap am = new ArgumentMap(args);
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
        String hibConfigFile = am.getValue("hibernate");
        configuration = new Configuration().configure(hibConfigFile);

        Integer nthreads = null;
        try {
            nthreads = Integer.parseInt(am.getValue("nthreads"));
        } catch (NumberFormatException nfe) {
            usage();
        }

        if (!correct) {
            System.exit(1);
        }

        String resource = configuration.getProperty("resource");
        String driver = configuration.getProperty("hibernate.connection.driver_class");
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

        if (!correct) {
            System.exit(1);
        }

        String username = configuration.getProperty("hibernate.connection.username");
        //String password = configuration.getProperty("hibernate.connection.password");
        String password = am.getValue("password");
        configuration.setProperty("hibernate.connection.password", password);
        String adsToken = configuration.getProperty("ads.token");
        String adsUrl = configuration.getProperty("ads.url");

        String connection = "jdbc:postgresql://" + host;
        configuration.setProperty("hibernate.connection.url", "jdbc:postgresql://" + host + ":" + port + "/" + database);

        ConfigProperties.getInstance().init(connection, driver, database, schema, host, port, username, password, adsUrl, adsToken);

        factory = configuration.buildSessionFactory();

        boolean proposalsChanged = true;
        log.info("config initiated");

        List<Callable<Proposal>> tasks = new ArrayList<>();
        List newProposals = new ArrayList<Proposal>();
        JSONArray proposals = null;
        try {
            String newRead = null;
            if (am.isSet("local")) {
                File file = new File("lastRead");
                if (file.exists() && !file.isDirectory()) {
                    try (BufferedReader br = new BufferedReader(new FileReader("lastRead"))) {
                        newRead = br.readLine();
                    } catch (IOException e) {
                        throw e;
                    }
                }

            } else {
                newRead = ProposalsReader.getInstance().read(resource);
                try (BufferedWriter bw = new BufferedWriter(new FileWriter("lastRead"))) {
                    bw.write(newRead);
                } catch (IOException e) {
                    throw e;
                }
            }
            JSONParser parser = new JSONParser();
            Object object = parser.parse(newRead);
            proposals = (JSONArray) object;
            log.info("number of proposals found in service = " + proposals.size());

            for (Object o : proposals) {
                tasks.add(new Worker((JSONObject) o));
            }

        } catch (ClassCastException | IOException | ParseException e) {
            correct = false;
            log.error("error parsing content from service " + e.getMessage());
        }

        if (!correct) {
            System.exit(1);
        }

        log.info("service read");

        if (!proposalsChanged) {
            log.info("no changes in the content provided by the service since last execution");
        } else {
            ExecutorService taskExecutor = null;
            try {
                taskExecutor = Executors.newFixedThreadPool(nthreads);
                List<Future<Proposal>> futures;

                futures = taskExecutor.invokeAll(tasks);

                for (Future<Proposal> f : futures) {
                    newProposals.add(f.get());
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error when executing thread in ThreadPool: " + e.getMessage() + " caused by: " + e.getCause().toString());
                correct = false;
            } finally {
                if (taskExecutor != null) {
                    taskExecutor.shutdown();
                }
            }
            if (!correct) {
                System.exit(1);
            }

            try {
                correct = tablesExist();
            } catch (SQLException | PropertyVetoException | UnableToCreatePorposalsTables e) {
                log.error("Error when checking existency of the tables or creating them: " + e.getMessage() + " caused by: " + e.getCause().toString());
                correct = false;
            }

            if (!correct) {
                System.exit(1);
            }
            log.info("tables exist");

            Session session = null;
            Transaction transaction = null;
            List currentProposals = null;
            try {
                session = factory.openSession();
                transaction = session.beginTransaction();
                currentProposals = session.createQuery("from Proposal").list();
                log.info("number of proposals read from DB " + currentProposals.size());

                Collections.sort(currentProposals, proposalComparator);
                Collections.sort(newProposals, proposalComparator);

                log.info("current proposals at first " + currentProposals.size());
                log.info("new proposals at first " + newProposals.size());

                List<Proposal> resultListToBeRemoved = processToBeRemoved(newProposals, currentProposals);
                List<Proposal> resultListToBeAdded = processToBeAdded(newProposals, currentProposals);
                log.info("proposals to be removed " + resultListToBeRemoved.size());
                log.info("proposals to be added " + resultListToBeAdded.size());

                if (resultListToBeRemoved.size() > 0) {
                    log.info("removing proposals");
                }
                for (Proposal p : resultListToBeRemoved) {
                    log.info("proposal removed " + p.getPropId());
                    session.remove(p);
                }

                if (transaction != null) {
                    transaction.commit();
                }
                transaction = session.beginTransaction();

                if (resultListToBeAdded.size() > 0) {
                    log.info("adding proposals");
                }
                for (Proposal p : resultListToBeAdded) {
                    log.info("proposal added " + p.getPropId());
                    session.saveOrUpdate(p);
                }

            } catch (Throwable ex) {
                log.error("Failed to create sessionFactory object." + ex);
                correct = false;
            } finally {
                try {
                    if (transaction != null) {
                        transaction.commit();
                    }
                    if (session != null) {
                        session.close();
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    correct = false;
                }
            }

            if (!correct) {
                System.exit(1);
            }

        }
        System.exit(0);
    }

    private static List<Proposal> processToBeAdded(List<Proposal> newProposals, List<Proposal> currentProposals) {
        log.info("checking for proposals to be added " + currentProposals.size());
        List<Proposal> proposalsToBeAdded = new ArrayList<Proposal>();
        for (Object p : newProposals) {
            //            log.info("new proposal " + ((Proposal) p).getPropId() + " to be added");
            Proposal pService = (Proposal) p;
            int foundInDB = Arrays.binarySearch(currentProposals.toArray(), pService);
            //            boolean foundInDB = currentProposals.contains(pService);
            if (foundInDB < 0) {
                proposalsToBeAdded.add(pService);
            }
        }
        return proposalsToBeAdded;
    }

    private static List<Proposal> processToBeRemoved(List<Proposal> newProposals, List<Proposal> currentProposals) {
        log.info("checking for proposals to be removed");
        List<Proposal> proposalsToBeRemoved = new ArrayList<Proposal>();
        for (int i = 0; i < currentProposals.size(); i++) {
            Proposal pDB = currentProposals.get(i);
            int foundInService = Arrays.binarySearch(newProposals.toArray(), pDB);
            //            boolean foundInService = newProposals.contains(pDB);
            if (foundInService < 0) {
                proposalsToBeRemoved.add(pDB);
            }
        }
        return proposalsToBeRemoved;
    }

    private static Comparator<Proposal> proposalComparator = new Comparator<Proposal>() {
        @Override
        public int compare(Proposal p1, Proposal p2) {
            return p1.getPropId().compareTo(p2.getPropId());
        }
    };

    private static boolean tablesExist() throws SQLException, PropertyVetoException, UnableToCreatePorposalsTables {
        boolean correct = true;
        String queryProposals = "select exists (select 1 from information_schema.tables where table_schema = '" + ConfigProperties.getInstance().getSchema()
                + "' and table_name = 'proposal')";
        String queryPublications = "select exists (select 1 from information_schema.tables where table_schema = '" + ConfigProperties.getInstance().getSchema()
                + "' and table_name = 'publication')";
        String queryPropPub = "select exists (select 1 from information_schema.tables where table_schema = '" + ConfigProperties.getInstance().getSchema()
                + "' and table_name = 'publication_proposal')";
        log.debug("queryProposals = " + queryProposals);
        log.debug("queryPublications = " + queryPublications);
        log.debug("queryPropPub = " + queryPropPub);
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        boolean existsProposals = false;
        boolean existsPublications = false;
        boolean existsPropPub = false;

        try {
            con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            rs = stmt.executeQuery(queryProposals);
            if (rs.next()) {
                existsProposals = rs.getBoolean(1);
            }
            log.debug(ConfigProperties.getInstance().getSchema() + ".proposal exists = " + existsProposals);
            rs.close();
            if (existsProposals) {
                rs = stmt.executeQuery(queryPublications);
                if (rs.next()) {
                    existsPublications = rs.getBoolean(1);
                }
                log.debug(ConfigProperties.getInstance().getSchema() + ".publication exists = " + existsPublications);
                rs.close();
                if (existsPublications) {
                    rs = stmt.executeQuery(queryPropPub);
                    if (rs.next()) {
                        existsPropPub = rs.getBoolean(1);
                    }
                    log.debug(ConfigProperties.getInstance().getSchema() + ".publication_proposal exists = " + existsPropPub);
                }
            }

        } catch (Exception ex) {
            throw new UnableToCreatePorposalsTables("Unexpected exception when checking proposals and publications table exist: " + ex.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                }
            }
        }
        correct = existsProposals && existsPublications && existsPropPub;
        return correct;
    }

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: esac-caom2-repo-publications [-v|--verbose|-d|--debug] [-h|--help] ...");
        sb.append("\n         --hibernate=path to the hibernate.cfg.xml file");
        sb.append("\n         --password=db password to be used");
        sb.append("\n         --threads=number of threads used to read papers");
        sb.append("\n         --local: use local file containing the set of proposals instead of the response of STScI service");
        log.warn(sb.toString());
    }

}
