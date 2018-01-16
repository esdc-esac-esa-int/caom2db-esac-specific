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
import java.util.Iterator;
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

    @SuppressWarnings("rawtypes")
    @Transactional(rollbackFor = Exception.class)
    public static void main(String[] args) {
        boolean correct = true;

        configuration = new Configuration().configure("/esac/archive/ehst/dl/caom2/repo/client/publications/hibernate.cfg.xml");
        factory = configuration.buildSessionFactory();

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
        if (!am.isSet("url") || !am.isSet("connection") || !am.isSet("database") || !am.isSet("driver") || !am.isSet("schema") || !am.isSet("host")
                || !am.isSet("port") || !am.isSet("username") || !am.isSet("password") || !am.isSet("nthreads")) {
            usage();
            correct = false;
        }

        Integer nthreads = null;
        try {
            nthreads = Integer.parseInt(am.getValue("nthreads"));
        } catch (NumberFormatException nfe) {
            usage();
        }

        if (!correct) {
            System.exit(1);
        }

        String url = am.getValue("url");
        String connection = am.getValue("connection");
        String driver = am.getValue("driver");
        String database = am.getValue("database");
        String schema = am.getValue("schema");
        String host = am.getValue("host");
        Integer port = null;
        try {
            port = Integer.parseInt(am.getValue("port"));
        } catch (Exception e) {
            usage();
            correct = false;
        }

        if (!correct) {
            System.exit(1);
        }

        String username = am.getValue("username");
        String password = am.getValue("password");

        ConfigProperties.getInstance().init(connection, driver, database, schema, host, port, username, password);

        boolean proposalsChanged = false;
        log.info("config initiated");

        List<Callable<Proposal>> tasks = new ArrayList<>();
        List<Proposal> proposalList = new ArrayList<Proposal>();
        JSONArray proposals = null;
        try {
            String oldRead = null;
            String newRead = ProposalsReader.getInstance().read(url);

            File file = new File("lastRead");
            if (file.exists() && !file.isDirectory()) {
                try (BufferedReader br = new BufferedReader(new FileReader("lastRead"))) {
                    oldRead = br.readLine();
                } catch (IOException e) {
                    throw e;
                }
            }

            if (!newRead.equals(oldRead)) {
                proposalsChanged = true;
            }

            if (proposalsChanged) {
                try (BufferedWriter bw = new BufferedWriter(new FileWriter("lastRead"))) {
                    bw.write(newRead);
                } catch (IOException e) {
                    throw e;
                }
                JSONParser parser = new JSONParser();
                Object object = parser.parse(newRead);
                proposals = (JSONArray) object;
                log.info("number of proposals found in service = " + proposals.size());

                for (Object o : proposals) {
                    tasks.add(new Worker((JSONObject) o));
                }
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
                    proposalList.add(f.get());
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

            log.info("proposals read from service");

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
                log.debug("opening hibernate session");
                session = factory.openSession();
                transaction = session.beginTransaction();
                currentProposals = session.createQuery("from Proposal").list();
                log.info("number of proposals found in database = " + currentProposals.size());

            } catch (Throwable ex) {
                log.error("Failed to create sessionFactory object." + ex);
                correct = false;
            } finally {
                if (transaction != null) {
                    transaction.commit();
                }
                if (session != null) {
                    session.close();
                }
            }

            if (!correct) {
                System.exit(1);
            }

            log.info("porposals read from database");

            for (Object pDB : currentProposals) {
                Proposal prop = (Proposal) pDB;
                log.debug("porposal read from database with prop_id = " + prop.getPropId());
                boolean found = false;
                for (Proposal pService : proposalList) {
                    if (!pService.equals(pDB))
                        continue;
                    found = true;
                    break;
                }
                if (!found) {

                }
                Iterator iter = prop.getPublications().iterator();
                while (iter.hasNext()) {
                    Publication pub = (Publication) iter.next();
                    log.debug("porposal " + prop.getPropId() + " read from database with publication title = " + pub.getTitle());
                }
            }
        }
        System.exit(0);
    }

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

    public boolean clean(List<Proposal> proposalsList) {
        boolean result = false;
        String selectProposals = null;
        String selectPublications = null;

        selectProposals = "select prop_id from " + ConfigProperties.getInstance().getSchema() + ".proposal";
        log.debug("selectProposals = " + selectProposals);

        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            rs = stmt.executeQuery(selectProposals);
            List<String> propIdList = new ArrayList<String>();
            List<String> propToBeRemoved = new ArrayList<String>();
            while (rs.next()) {
                propIdList.add(rs.getString(1));
            }

            for (String p : propIdList) {
                boolean found = false;
                for (Proposal prop : proposalsList) {
                    if (!p.equals(prop.getPropId())) {
                        continue;
                    }
                    found = true;
                    break;
                }
                if (!found) {
                    propToBeRemoved.add(p);
                }
            }

            removeOldProposals(propToBeRemoved, con, stmt);

        } catch (Exception ex) {
            throw new RuntimeException("Unexpected exception removing not valid proposals: " + ex.getMessage());
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
        return result;
    }

    private void removeOldProposals(List<String> propToBeRemoved, Connection con, Statement stmt) {
        // TODO Auto-generated method stub

    }

    //    public boolean upsert(Proposal proposal) {
    //        // INSERT INTO tablename (a, b, c) values (1, 2, 10) ON CONFLICT (a) DO
    //        // UPDATE SET c = tablename.c + 1;
    //        boolean result = false;
    //        String insertProposal = null;
    //        String insertPublicaciones = null;
    //
    //        //      insertProposal = "insert into " + ConfigProperties.getInstance().getSchema()
    //        //      + ".proposal (prop_id, fname, mi, lname, title, type_, cycle, sci_cat, abstract) values ('" + proposal.getPropId() + "', '"
    //        //      + proposal.getFname() + "', '" + proposal.getMi() + "', '" + proposal.getLname() + "', '" + proposal.getTitle() + "', '" + proposal.getType()
    //        //      + "', '" + proposal.getSciCat() + "', '" + proposal.getPubAbstract() + "') where not exists (select 1 from "
    //        //      + ConfigProperties.getInstance().getSchema() + ".proposal where prop_id = '" + proposal.getPropId() + "'";
    //        insertProposal = "insert into " + ConfigProperties.getInstance().getSchema()
    //                + ".proposal (prop_id, fname, mi, lname, title, type_, cycle, sci_cat, abstract) values ('" + proposal.getPropId() + "', '"
    //                + proposal.getFname() + "', '" + proposal.getMi() + "', '" + proposal.getLname() + "', '" + proposal.getTitle() + "', '" + proposal.getType()
    //                + "', '" + proposal.getSciCat() + "', '" + proposal.getPubAbstract() + "') on conflict (prop_id) do update set (fname = '" + proposal.getLname()
    //                + "', mi = '" + proposal.getMi() + "', lname = '" + proposal.getLname() + "', title = '" + proposal.getTitle() + "', type_ = '"
    //                + proposal.getType() + "', cycle = '" + proposal.getCycle() + "', sci_cat = " + proposal.getSciCat() + "', abstract = "
    //                + proposal.getPubAbstract() + "'";
    //        log.debug("insertProposal = " + insertProposal);
    //
    //        Connection con = null;
    //        Statement stmt = null;
    //        try {
    //            con = JdbcSingleton.getInstance().getConnection();
    //            stmt = con.createStatement();
    //            int res = stmt.executeUpdate(insertProposal);
    //            if (res != 1) {
    //                throw new RuntimeException("Unexpected exception inserting proposal: " + proposal.getPropId().toString());
    //            }
    //        } catch (Exception ex) {
    //            throw new RuntimeException("Unexpected exception inserting proposal: " + proposal.getPropId().toString());
    //        } finally {
    //            if (stmt != null) {
    //                try {
    //                    stmt.close();
    //                } catch (SQLException e) {
    //                }
    //            }
    //            if (con != null) {
    //                try {
    //                    con.close();
    //                } catch (SQLException e) {
    //                }
    //            }
    //        }
    //        return result;
    //    }

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: esac-caom2-repo-publications [-v|--verbose|-d|--debug] [-h|--help] ...");
        sb.append("\n         --url=url to retrieve data from (e.g. https://mastpartners.stsci.edu/partners/papers)");
        sb.append("\n         --connection=connection to the database (e.g. jdbc:postgresql://hstdev02.n1data.lan");
        sb.append("\n         --driver=driver of the database (e.g. org.postgresql.Driver");
        sb.append("\n         --database=name of the database (e.g. ehst_dev");
        sb.append("\n         --schema=schema in database to be used (e.g. caom2");
        sb.append("\n         --host=host where the database is located (e.g. localhost)");
        sb.append("\n         --port=port where database manager is listening (e.g. 8300)");
        sb.append("\n         --username=username to access the database (e.g. postgres)");
        sb.append("\n         --password=password to access the database");
        sb.append("\n         --threads : number of threads used to read papers");
        log.warn(sb.toString());
    }

}
