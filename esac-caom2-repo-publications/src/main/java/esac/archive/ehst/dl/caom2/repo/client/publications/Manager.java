package esac.archive.ehst.dl.caom2.repo.client.publications;

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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
public class Manager {
    private static final Logger log = Logger.getLogger(Manager.class);

    @SuppressWarnings("unchecked")
    public static void addNewProposals(Session session, List<Proposal> currentProposals, List<Proposal> allProposals, Integer nThreads) {
        List<Publication> currentPublications = session.createQuery("from Publication").list();
        List<PublicationWorker> pubWorkers = new ArrayList<PublicationWorker>();

        Transaction transaction = session.beginTransaction();
        try {
            int index = 0;
            PublicationWorker.setCurrentPublications(currentPublications);
            for (Object o : allProposals) {
                Proposal p = (Proposal) o;
                if (!currentProposals.contains(p)) {
                    PublicationWorker worker = new PublicationWorker(p);
                    pubWorkers.add(worker);
                    index++;
                    if (ConfigProperties.getInstance().getConnsToADS() == index) {
                        break;
                    }
                }
            }

            allProposals.clear();

            for (PublicationWorker worker : pubWorkers) {
                try {
                    Proposal newP = worker.call();
                    if (newP.getPublications() == null) {
                    }
                    allProposals.add(newP);
                    log.info("adding proposal " + worker.getProposal().getPropId());
                } catch (Exception e) {
                    throw new Exception("Was not possible to read publications for proposal " + worker.getProposal().getPropId() + ": " + e.getMessage());
                }
            }

            if (allProposals.size() > 0) {
                log.info("saving proposals");
            }
            for (Object o : allProposals) {
                Proposal p = (Proposal) o;
                session.saveOrUpdate(p);
            }

        } catch (Throwable ex) {
            log.error("Error when reading ADS: " + ex.getMessage());
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
            }
        }
    }

    public static void removeOldProposals(Session session, List<Proposal> currentProposals, List<Proposal> allProposals) {
        Transaction transaction = session.beginTransaction();
        try {
            if (currentProposals.size() > 0) {
                for (Object o : currentProposals) {
                    Proposal p = (Proposal) o;
                    if (!allProposals.contains(p)) {
                        log.info("proposal removed " + p.getPropId());
                        session.remove(p);
                    }
                }
            }
        } catch (Exception ex) {
            log.error("Error removing old proposals: " + ex.getMessage());
        } finally {
            if (transaction != null) {
                transaction.commit();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static List<Proposal> readCurrentProposals(Session session) {
        boolean correct = true;
        List<Proposal> currentProposals = null;
        //        List<Publication> currentPublications = null;
        try {
            currentProposals = session.createQuery("from Proposal").list();
            for (Object o : currentProposals) {
                Proposal p = (Proposal) o;
                for (Publication pub : p.getPublications()) {
                    p.addBibcodes(pub.getBibcode());
                }
            }

            log.info("number of proposals read from DB " + currentProposals.size());
        } catch (Exception e) {
            log.error("Error reading database: " + e.getMessage());
            correct = false;
        }
        if (correct) {
            return currentProposals;
        } else {
            return null;
        }
    }

    public static List<Proposal> readAllProposals(String resource, Integer nThreads) {
        boolean correct = true;
        boolean proposalsChanged = false;

        List<Callable<Proposal>> tasksProposals = new ArrayList<>();
        List<Proposal> newProposals = new ArrayList<Proposal>();
        JSONArray proposals = null;
        String newRead = null;
        String lastRead = null;
        File file = new File("lastRead");
        boolean local = ConfigProperties.getInstance().isLocal();
        boolean noFile = !(file.exists() && !file.isDirectory());
        boolean readFromLocal = local && !noFile;

        try {
            if (readFromLocal) {
                try (BufferedReader br = new BufferedReader(new FileReader("lastRead"))) {
                    lastRead = br.readLine();
                    if (local) {
                        newRead = lastRead;
                    }
                } catch (IOException e) {
                    throw e;
                }
            } else {
                newRead = ProposalsReader.getInstance().read(resource);
            }

            proposalsChanged = !readFromLocal || lastRead == null || !lastRead.equals(newRead);
            lastRead = null;
            System.gc();
            if (proposalsChanged || readFromLocal) {
                if (newRead != null) {
                    if (!readFromLocal) {
                        try (BufferedWriter bw = new BufferedWriter(new FileWriter("lastRead"))) {
                            bw.write(newRead);
                        } catch (IOException e) {
                            throw e;
                        }
                    }
                    JSONParser parser = new JSONParser();
                    Object object = parser.parse(newRead);
                    newRead = null;
                    System.gc();
                    proposals = (JSONArray) object;
                    if (readFromLocal) {
                        log.info("number of proposals found locally = " + proposals.size());
                    } else {
                        log.info("number of proposals found in service = " + proposals.size());
                    }

                    for (Object o : proposals) {
                        tasksProposals.add(new ProposalWorker((JSONObject) o));
                    }
                }
            }

        } catch (ClassCastException | IOException | ParseException e) {
            correct = false;
            log.error("error parsing content from service " + e.getMessage());
            e.printStackTrace();
        }

        if (correct) {
            if (!proposalsChanged && !readFromLocal) {
                log.info("no changes in the content provided by the service since last execution");
                newProposals = null;
            } else {
                ExecutorService taskExecutor = null;
                try {
                    taskExecutor = Executors.newFixedThreadPool(nThreads);
                    List<Future<Proposal>> futures;

                    futures = taskExecutor.invokeAll(tasksProposals);

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
            }
        }
        return newProposals;
    }

    public static boolean tablesExist() throws SQLException, PropertyVetoException, UnableToCreatePorposalsTables {
        boolean correct = true;
        String queryProposals = "select exists (select 1 from information_schema.tables where table_schema = '" + ConfigProperties.getInstance().getSchema()
                + "' and table_name = 'proposal')";
        String queryPublications = "select exists (select 1 from information_schema.tables where table_schema = '" + ConfigProperties.getInstance().getSchema()
                + "' and table_name = 'publication')";
        String queryPropPub = "select exists (select 1 from information_schema.tables where table_schema = '" + ConfigProperties.getInstance().getSchema()
                + "' and table_name = 'publication_proposal')";
        //        log.debug("queryProposals = " + queryProposals);
        //        log.debug("queryPublications = " + queryPublications);
        //        log.debug("queryPropPub = " + queryPropPub);
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

}
