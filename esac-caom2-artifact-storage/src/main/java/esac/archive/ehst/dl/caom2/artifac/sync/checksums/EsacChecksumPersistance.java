package esac.archive.ehst.dl.caom2.artifac.sync.checksums;

import java.beans.PropertyVetoException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import esac.archive.ehst.dl.caom2.artifac.sync.checksums.db.ConfigProperties;
import esac.archive.ehst.dl.caom2.artifac.sync.checksums.db.JdbcSingleton;

/**
 *
 * @author jduran
 *
 */
public class EsacChecksumPersistance {

    private static Logger log = Logger.getLogger(EsacChecksumPersistance.class.getName());

    protected static final String TABLENAME = "caom2.artifactsync.checksumTable.table";
    protected static final String COLUMN_ARTIFACT = "caom2.artifactsync.checksumTable.columnArtifact";
    protected static final String COLUMN_CHECKSUM = "caom2.artifactsync.checksumTable.columnChecksum";
    protected static final String PK = "caom2.artifactsync.checksumTable.pk";

    private static String checksumSchema = null;
    private static String checksumTable = null;
    private static String checksumArtifactColumnName = null;
    private static String checksumChecksumColumnName = null;
    private static String checksumPk = null;

    private static EsacChecksumPersistance instance = null;

    public static EsacChecksumPersistance getInstance() {
        if (instance == null) {
            instance = new EsacChecksumPersistance();
        }
        return instance;
    }

    private EsacChecksumPersistance() {
        try {
            setChecksumSchema(JdbcSingleton.getInstance().getDbschema());
            setChecksumTable(ConfigProperties.getInstance().getProperty(TABLENAME));
            setChecksumArtifactColumnName(ConfigProperties.getInstance().getProperty(COLUMN_ARTIFACT));
            setChecksumChecksumColumnName(ConfigProperties.getInstance().getProperty(COLUMN_CHECKSUM));
            setChecksumPk(ConfigProperties.getInstance().getProperty(PK));

            boolean tableExists = checkIfTableExists(getChecksumSchema(), getChecksumTable());
            if (!tableExists) {
                JdbcSingleton.getInstance();
                createChecksumTable();
                tableExists = checkIfTableExists(getChecksumSchema(), getChecksumTable());
                if (!tableExists) {
                    throw new Exception(
                            "Table " + getChecksumSchema() + "." + getChecksumTable() + " doesn't exist. Check configuration file and permissions in database");
                }

            }

        } catch (Exception e) {
            log.error("Unexpected exception constructing EsacChecksumPersistance: " + e.getMessage());
            System.exit(1);
        }
    }

    private void createChecksumTable() throws SQLException {
        Connection con = null;
        Statement stmt = null;
        try {
            con = JdbcSingleton.getInstance().getConnection();
            log.debug("****************** creating checksum table");

            String create = "create table " + checksumSchema + "." + checksumTable + " (" + checksumArtifactColumnName + " character varying(1024) not null, "
                    + checksumChecksumColumnName + " character varying(64), constraint " + checksumPk + " primary key (" + checksumArtifactColumnName
                    + ")) with (OIDS=FALSE); alter table " + checksumSchema + "." + checksumTable + " owner to " + JdbcSingleton.getInstance().getOwner() + ";";

            String index = "create index on " + checksumSchema + "." + checksumTable + " using btree (" + checksumArtifactColumnName + ");";
            String index2 = "create index on " + checksumSchema + "." + checksumTable + " using btree (" + checksumChecksumColumnName + ");";
            stmt = con.createStatement();
            stmt.execute(create);
            stmt.execute(index);
            stmt.execute(index2);

        } catch (Exception err) {
            log.error("Unexpected exception creating checksum table: " + err.getMessage());
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

    public boolean upsert(URI artifactURI, URI md5) throws SQLException {
        log.debug("****** upsert of duple ('" + artifactURI + "', " + md5 + "')");

        // INSERT INTO tablename (a, b, c) values (1, 2, 10) ON CONFLICT (a) DO
        // UPDATE SET c = tablename.c + 1;
        boolean result = false;
        String upsert = null;
        if (md5 != null) {
            String md5md5 = "md5:" + md5.toString();
            upsert = "insert into " + getChecksumSchema() + "." + getChecksumTable() + " (" + getChecksumArtifactColumnName() + ", "
                    + getChecksumChecksumColumnName() + ") " + " values ('" + artifactURI.toString() + "', '" + md5md5.toString() + "') on conflict ("
                    + getChecksumArtifactColumnName() + ") do update set " + getChecksumChecksumColumnName() + " = '" + md5md5.toString() + "';";
        } else {
            upsert = "insert into " + getChecksumSchema() + "." + getChecksumTable() + " (" + getChecksumArtifactColumnName() + ", "
                    + getChecksumChecksumColumnName() + ") " + " values ('" + artifactURI.toString() + "', NULL) on conflict ("
                    + getChecksumArtifactColumnName() + ") do update set " + getChecksumChecksumColumnName() + " = NULL;";
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
                throw new Exception("Unexpected exception inserting artifact: " + artifactURI.toString());
            }
        } catch (Exception ex) {
            log.error(ex.getMessage());
            ex.printStackTrace();
            System.exit(2);
        } finally {
            if (stmt != null) {
            	stmt.close();
            }
            if (con != null) {
                con.close();
            }
        }
        return result;
    }

    public boolean select(URI artifactURI) throws SQLException {
        boolean result = false;
        Statement stmt = null;
        String query = "select * from " + getChecksumSchema() + "." + getChecksumTable() + " where " + getChecksumArtifactColumnName() + " = '"
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
            ex.printStackTrace();
            System.exit(2);
        } finally {
            if (rs != null) {
            	rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                con.close();
            }
        }
        return result;

    }

    public boolean select(URI artifactURI, URI checksum) throws SQLException {
        boolean result = false;
        Statement stmt = null;
        String md5Checksum = checksum.toString();
        if (!md5Checksum.startsWith("md5:")) {
            md5Checksum = "md5:" + checksum.toString();
        }
        String query = "select * from " + getChecksumSchema() + "." + getChecksumTable() + " where " + getChecksumArtifactColumnName() + " = '"
                + artifactURI.toString() + "' and " + getChecksumChecksumColumnName() + " = '" + md5Checksum.toString() + "'";
        log.info("select query = " + query);
        ResultSet rs = null;

        Connection con = null;
        try {
            con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            rs = stmt.executeQuery(query);
            result = rs.next();
        } catch (Exception ex) {
            log.error("Unexpected exception when selecting: " + ex.getMessage());
            ex.printStackTrace();
            System.exit(2);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                con.close();
            }
        }
        return result;
    }

    private boolean checkIfTableExists(String checksumSchema, String checksumTable) throws SQLException, PropertyVetoException {
        String query = "select exists (select 1 from information_schema.tables where table_schema = '" + checksumSchema + "' and table_name = '" + checksumTable
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
            log.debug("exists = " + exists);

        } catch (Exception ex) {
            log.error("Unexpected exception when selecting: " + ex.getMessage());
            ex.printStackTrace();
            System.exit(2);
        } finally {
            if (rs != null) {
               rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                con.close();
            }
        }
        return exists;
    }

    public String getChecksumSchema() {
        return checksumSchema;
    }

    private void setChecksumSchema(String checksumSchema) {
        EsacChecksumPersistance.checksumSchema = checksumSchema;
    }

    public String getChecksumTable() {
        return checksumTable;
    }

    private void setChecksumTable(String checksumTable) {
        EsacChecksumPersistance.checksumTable = checksumTable;
    }

    public String getChecksumArtifactColumnName() {
        return checksumArtifactColumnName;
    }

    private void setChecksumArtifactColumnName(String checksumArtifactColumnName) {
        EsacChecksumPersistance.checksumArtifactColumnName = checksumArtifactColumnName;
    }

    public String getChecksumChecksumColumnName() {
        return checksumChecksumColumnName;
    }

    private void setChecksumChecksumColumnName(String checksumChecksumColumnName) {
        EsacChecksumPersistance.checksumChecksumColumnName = checksumChecksumColumnName;
    }

    public String getChecksumPk() {
        return checksumPk;
    }

    private static void setChecksumPk(String checksumPk) {
        EsacChecksumPersistance.checksumPk = checksumPk;
    }
}