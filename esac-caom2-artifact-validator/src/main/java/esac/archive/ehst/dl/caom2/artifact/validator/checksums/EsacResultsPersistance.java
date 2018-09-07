package esac.archive.ehst.dl.caom2.artifact.validator.checksums;

import java.beans.PropertyVetoException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import esac.archive.ehst.dl.caom2.artifact.validator.checksums.db.ConfigProperties;
import esac.archive.ehst.dl.caom2.artifact.validator.checksums.db.JdbcSingleton;

/**
 *
 * @author jduran
 *
 */
public class EsacResultsPersistance {

    private static Logger log = Logger.getLogger(EsacResultsPersistance.class.getName());

    protected static final String TABLENAME = "caom2.artifactsync.resultsTable.table";
    protected static final String COLUMN_ARTIFACT = "caom2.artifactsync.resultsTable.columnArtifact";
    protected static final String COLUMN_EXPECTEDCHECKSUM = "caom2.artifactsync.resultsTable.columnExpectedChecksum";
    protected static final String COLUMN_CALCULATEDCHECKSUM = "caom2.artifactsync.resultsTable.columnCalculatedChecksum";
    protected static final String PK = "caom2.artifactsync.resultsTable.pk";

    private static String resultsSchema = null;
    private static String resultsTable = null;
    private static String resultsArtifactColumnName = null;
    private static String resultsExpectedChecksumColumnName = null;
    private static String resultsCalculatedChecksumColumnName = null;
    private static String resultsPk = null;

    private static EsacResultsPersistance instance = null;

    public static EsacResultsPersistance getInstance() {
        if (instance == null) {
            instance = new EsacResultsPersistance();
        }
        return instance;
    }

    private EsacResultsPersistance() {
        try {
            setResultsSchema(JdbcSingleton.getInstance().getDbschema());
            setResultsTable(ConfigProperties.getInstance().getProperty(TABLENAME));
            setResultsArtifactColumnName(ConfigProperties.getInstance().getProperty(COLUMN_ARTIFACT));
            setResultsExpectedChecksumColumnName(ConfigProperties.getInstance().getProperty(COLUMN_EXPECTEDCHECKSUM));
            setResultsCalculatedChecksumColumnName(ConfigProperties.getInstance().getProperty(COLUMN_CALCULATEDCHECKSUM));
            setResultsPk(ConfigProperties.getInstance().getProperty(PK));

            boolean tableExists = checkIfTableExists();
            if (!tableExists) {
                JdbcSingleton.getInstance();
                createResultsTable();
                tableExists = checkIfTableExists();
                if (!tableExists) {
                    throw new Exception(
                            "Table " + getResultsSchema() + "." + getResultsTable() + " doesn't exist. Check configuration file and permissions in database");
                }

            }

        } catch (Exception e) {
            log.error("Unexpected exception constructing EsacResultsPersistance: " + e.getMessage());
            System.exit(1);
        }
    }

    private void createResultsTable() throws SQLException {
        Connection con = null;
        Statement stmt = null;
        try {
            con = JdbcSingleton.getInstance().getConnection();
            log.debug("****************** creating results table");

            String create = "create table " + resultsSchema + "." + resultsTable + " (" + resultsArtifactColumnName + " character varying(64) not null, "
                    + resultsExpectedChecksumColumnName + " character varying(64) not null, " + resultsCalculatedChecksumColumnName
                    + " character varying(64) not null, " + " constraint " + resultsPk + " primary key (" + resultsArtifactColumnName
                    + ")) with (OIDS=FALSE); alter table " + resultsSchema + "." + resultsTable + " owner to " + JdbcSingleton.getInstance().getOwner() + ";";

            String index = "create index on " + resultsSchema + "." + resultsTable + " using btree (" + resultsArtifactColumnName + ");";
            String index2 = "create index on " + resultsSchema + "." + resultsTable + " using btree (" + resultsCalculatedChecksumColumnName + ");";
            stmt = con.createStatement();
            stmt.execute(create);
            stmt.execute(index);
            stmt.execute(index2);

        } catch (Exception err) {
            log.error("Unexpected exception creating results table: " + err.getMessage());
            err.printStackTrace();
            System.exit(2);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                con.close();
            }
        }
    }

    public boolean upsert(URI artifactURI, URI expectedMd5, URI calculatedMd5) {
        log.debug("****** upsert of tuple ('" + artifactURI + "', " + expectedMd5 + "', " + calculatedMd5 + "')");

        // INSERT INTO tablename (a, b, c) values (1, 2, 10) ON CONFLICT (a) DO
        // UPDATE SET c = tablename.c + 1;
        boolean result = false;
        String upsert = null;
        String expectedmd5md5 = null;
        String calculatedmd5md5 = null;
        if (expectedMd5 != null) {
            expectedmd5md5 = expectedMd5.toString();
            if (!expectedmd5md5.toString().startsWith("md5:")) {
                expectedmd5md5 = "md5:" + expectedMd5.toString();
            }
        }
        if (calculatedMd5 != null) {
            calculatedmd5md5 = calculatedMd5.toString();
            if (!calculatedmd5md5.toString().startsWith("md5:")) {
                calculatedmd5md5 = "md5:" + calculatedMd5.toString();
            }
        }
        if (expectedMd5 != null && calculatedMd5 != null) {
            upsert = "insert into " + getResultsSchema() + "." + getResultsTable() + " (" + getResultsArtifactColumnName() + ", "
                    + getResultsExpectedChecksumColumnName() + ", " + getResultsCalculatedChecksumColumnName() + ") " + " values ('" + artifactURI.toString()
                    + "', '" + expectedmd5md5.toString() + "', '" + calculatedmd5md5.toString() + "') on conflict (" + getResultsArtifactColumnName()
                    + ") do update set " + getResultsExpectedChecksumColumnName() + " = '" + expectedmd5md5.toString() + "', "
                    + getResultsCalculatedChecksumColumnName() + " = '" + expectedmd5md5.toString() + "';";
        } else if (expectedMd5 == null) {
            upsert = "insert into " + getResultsSchema() + "." + getResultsTable() + " (" + getResultsArtifactColumnName() + ", "
                    + getResultsExpectedChecksumColumnName() + ", " + getResultsCalculatedChecksumColumnName() + ") " + " values ('" + artifactURI.toString()
                    + "', NULL " + ", '" + calculatedmd5md5.toString() + "') on conflict (" + getResultsArtifactColumnName() + ") do update set "
                    + getResultsExpectedChecksumColumnName() + " = NULL ," + getResultsCalculatedChecksumColumnName() + " = '" + calculatedmd5md5.toString()
                    + "';";
        } else if (calculatedMd5 == null) {
            upsert = "insert into " + getResultsSchema() + "." + getResultsTable() + " (" + getResultsArtifactColumnName() + ", "
                    + getResultsExpectedChecksumColumnName() + ", " + getResultsCalculatedChecksumColumnName() + ") " + " values ('" + artifactURI.toString()
                    + "', '" + expectedmd5md5.toString() + "', NULL " + ") on conflict (" + getResultsArtifactColumnName() + ") do update set "
                    + getResultsCalculatedChecksumColumnName() + " = NULL ," + getResultsExpectedChecksumColumnName() + " = '" + expectedmd5md5.toString()
                    + "';";
        } else {
            upsert = "insert into " + getResultsSchema() + "." + getResultsTable() + " (" + getResultsArtifactColumnName() + ", "
                    + getResultsExpectedChecksumColumnName() + ", " + getResultsCalculatedChecksumColumnName() + ") " + " values ('" + artifactURI.toString()
                    + "', NULL, NULL " + ") on conflict (" + getResultsArtifactColumnName() + ") do update set " + getResultsCalculatedChecksumColumnName()
                    + " = NULL ," + getResultsExpectedChecksumColumnName() + " = NULL;";
        }

        log.info("upsert query = " + upsert);

        Connection con = null;
        Statement stmt = null;
        try {
            con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            int res = stmt.executeUpdate(upsert);
            if (res != 1) {
                log.error("Unexpected exception inserting artifact: " + artifactURI.toString());
                result = false;
            }
        } catch (Exception ex) {
            log.error("Unexpected exception when inserting: " + ex.getMessage());
            result = false;
        } finally {
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

    public boolean select(URI artifactURI) {
        boolean result = false;
        Statement stmt = null;
        String query = "select * from " + getResultsSchema() + "." + getResultsTable() + " where " + getResultsArtifactColumnName() + " = '"
                + artifactURI.toString() + "';";
        ResultSet rs = null;

        Connection con = null;
        try {
            con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            rs = stmt.executeQuery(query);
            result = rs.next();
        } catch (Exception ex) {
            log.error("Unexpected exception when selecting: " + ex.getMessage());
            result = false;
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

    private boolean checkIfTableExists() throws SQLException, PropertyVetoException {
        String query = "select exists (select 1 from information_schema.tables where table_schema = '" + resultsSchema + "' and table_name = '" + resultsTable
                + "')";
        log.debug("query = " + query);
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        boolean exists = false;

        try {
            con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            rs = stmt.executeQuery(query);
            if (rs.next()) {
                exists = rs.getBoolean(1);
            }
            log.info("exists = " + exists);

        } catch (Exception ex) {
            log.error("Unexpected exception when selecting: " + ex.getMessage());
            exists = false;
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
        return exists;
    }

    public String getResultsSchema() {
        return resultsSchema;
    }

    private void setResultsSchema(String resultsSchema) {
        EsacResultsPersistance.resultsSchema = resultsSchema;
    }

    public String getResultsTable() {
        return resultsTable;
    }

    private void setResultsTable(String resultsTable) {
        EsacResultsPersistance.resultsTable = resultsTable;
    }

    public String getResultsArtifactColumnName() {
        return resultsArtifactColumnName;
    }

    private void setResultsArtifactColumnName(String resultsArtifactColumnName) {
        EsacResultsPersistance.resultsArtifactColumnName = resultsArtifactColumnName;
    }

    public String getResultsExpectedChecksumColumnName() {
        return resultsExpectedChecksumColumnName;
    }

    private void setResultsExpectedChecksumColumnName(String resultsExpectedChecksumColumnName) {
        EsacResultsPersistance.resultsExpectedChecksumColumnName = resultsExpectedChecksumColumnName;
    }

    public String getResultsPk() {
        return resultsPk;
    }

    private static void setResultsPk(String resultsPk) {
        EsacResultsPersistance.resultsPk = resultsPk;
    }

    /**
     * @return the resultsCalculatedChecksumColumnName
     */
    public static String getResultsCalculatedChecksumColumnName() {
        return resultsCalculatedChecksumColumnName;
    }

    /**
     * @param resultsCalculatedChecksumColumnName
     *            the resultsCalculatedChecksumColumnName to set
     */
    public static void setResultsCalculatedChecksumColumnName(String resultsCalculatedChecksumColumnName) {
        EsacResultsPersistance.resultsCalculatedChecksumColumnName = resultsCalculatedChecksumColumnName;
    }
}