package esac.archive.ehst.dl.caom2.artifac.sync.checksums;

import java.beans.PropertyVetoException;
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
public class EsacResultsPersistance {

    private static Logger log = Logger.getLogger(EsacResultsPersistance.class.getName());

    protected static final String TABLENAME = "caom2.artifactsync.resultsTable.table";
    protected static final String COLUMN_DATE = "caom2.artifactsync.resultsTable.columnDate";
    protected static final String COLUMN_TOTALFILES = "caom2.artifactsync.resultsTable.columnTotalFiles";
    protected static final String COLUMN_SUCCESSFILES = "caom2.artifactsync.resultsTable.columnSuccessFiles";
    protected static final String COLUMN_ELAPSEDTIME = "caom2.artifactsync.resultsTable.columnElapsedTime";
    protected static final String COLUMN_BYTES = "caom2.artifactsync.resultsTable.columnBytes";
    protected static final String COLUMN_THREADS = "caom2.artifactsync.resultsTable.columnThreads";
    protected static final String COLUMN_PERFORMANCE = "caom2.artifactsync.resultsTable.columnPerformance";
    protected static final String COLUMN_UNIT = "caom2.artifactsync.resultsTable.columnUnit";
    protected static final String COLUMN_MESSAGE = "caom2.artifactsync.resultsTable.columnMessage";
    protected static final String PK = "caom2.artifactsync.resultsTable.pk";

    private static String resultsSchema = null;
    private static String resultsTable = null;
    private static String resultsDateColumnName = null;
    private static String resultsTotalFilesColumnName = null;
    private static String resultsSuccessFilesColumnName = null;
    private static String resultsElapsedTimeColumnName = null;
    private static String resultsBytesColumnName = null;
    private static String resultsThreadsColumnName = null;
    private static String resultsPerformanceColumnName = null;
    private static String resultsUnitColumnName = null;
    private static String resultsMessageColumnName = null;
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
            setResultsDateColumnName(ConfigProperties.getInstance().getProperty(COLUMN_DATE));
            setResultsTotalFilesColumnName(ConfigProperties.getInstance().getProperty(COLUMN_TOTALFILES));
            setResultsSuccessFilesColumnName(ConfigProperties.getInstance().getProperty(COLUMN_SUCCESSFILES));
            setResultsElapsedTimeColumnName(ConfigProperties.getInstance().getProperty(COLUMN_ELAPSEDTIME));
            setResultsBytesColumnName(ConfigProperties.getInstance().getProperty(COLUMN_BYTES));
            setResultsThreadsColumnName(ConfigProperties.getInstance().getProperty(COLUMN_THREADS));
            setResultsPerformanceColumnName(ConfigProperties.getInstance().getProperty(COLUMN_PERFORMANCE));
            setResultsUnitColumnName(ConfigProperties.getInstance().getProperty(COLUMN_UNIT));
            setResultsMessageColumnName(ConfigProperties.getInstance().getProperty(COLUMN_MESSAGE));
            setChecksumPk(ConfigProperties.getInstance().getProperty(PK));

            boolean tableExists = checkIfTableExists(getResultsSchema(), getResultsTable());
            if (!tableExists) {
                JdbcSingleton.getInstance();
                createResultsTable();
                tableExists = checkIfTableExists(getResultsSchema(), getResultsTable());
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

            String create = "create table " + resultsSchema + "." + resultsTable + " (" + resultsDateColumnName + " character varying(1024) not null, "
                    + resultsTotalFilesColumnName + " bigint, " + resultsSuccessFilesColumnName + " bigint, " + resultsElapsedTimeColumnName + " bigint, "
                    + resultsBytesColumnName + " bigint, " + resultsThreadsColumnName + " bigint, " + resultsPerformanceColumnName + " real, "
                    + resultsUnitColumnName + " character varying(64), " + resultsMessageColumnName + " character varying(1024), constraint " + resultsPk
                    + " primary key (" + resultsDateColumnName + ")) with (OIDS=FALSE); alter table " + resultsSchema + "." + resultsTable + " owner to "
                    + JdbcSingleton.getInstance().getOwner() + ";";

            String index = "create index on " + resultsSchema + "." + resultsTable + " using btree (" + resultsPerformanceColumnName + ");";
            String index2 = "create index on " + resultsSchema + "." + resultsTable + " using btree (" + resultsSuccessFilesColumnName + ");";
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

    public boolean insert(String date, long totalFiles, long successFiles, long elapsedTime, long bytes, long threads, float performance, String units,
            String message) throws SQLException {
        log.debug("****** upsert of row ('" + date + "')");

        // INSERT INTO tablename (a, b, c) values (1, 2, 10) ON CONFLICT (a) DO
        // UPDATE SET c = tablename.c + 1;
        boolean result = false;
        String insert = null;
        String modif_units = (units == null) ? "null" : "'" + units + "'";
        String modif_message = (message == null) ? "null" : "'" + message + "'";
        insert = "insert into " + resultsSchema + "." + resultsTable + " (" + getResultsDateColumnName() + ", " + getResultsTotalFilesColumnName() + ", "
                + getResultsSuccessFilesColumnName() + ", " + getResultsElapsedTimeColumnName() + ", " + getResultsBytesColumnName() + ", "
                + getResultsThreadsColumnName() + ", " + getResultsPerformanceColumnName() + ", " + getResultsUnitColumnName() + ", "
                + getResultsMessageColumnName() + ") " + " values ('" + date + "', '" + totalFiles + "', '" + successFiles + "', '" + elapsedTime + "', '"
                + bytes + "', '" + threads + "', '" + performance + "', " + modif_units + ", " + modif_message + ");";

        log.info("insert query = " + insert);

        Connection con = null;
        Statement stmt = null;
        try {
            con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            int res = stmt.executeUpdate(insert);
            if (res != 1) {
                log.error("Unexpected exception inserting date: " + date);
                throw new Exception("Unexpected exception inserting date: " + date);
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

    private boolean checkIfTableExists(String schema, String table) throws SQLException, PropertyVetoException {
        String query = "select exists (select 1 from information_schema.tables where table_schema = '" + schema + "' and table_name = '" + table + "')";
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
                try {
                    rs.close();
                } catch (SQLException e) {
                }
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

    public String getResultsSchema() {
        return EsacResultsPersistance.resultsSchema;
    }

    private void setResultsSchema(String schema) {
        EsacResultsPersistance.resultsSchema = schema;
    }

    public String getResultsTable() {
        return EsacResultsPersistance.resultsTable;
    }

    private void setResultsTable(String table) {
        EsacResultsPersistance.resultsTable = table;
    }

    public String getResultsPk() {
        return EsacResultsPersistance.resultsPk;
    }

    private static void setChecksumPk(String pk) {
        EsacResultsPersistance.resultsPk = pk;
    }

    /**
     * @return the resultsDateColumnName
     */
    public static String getResultsDateColumnName() {
        return resultsDateColumnName;
    }

    /**
     * @param resultsDateColumnName
     *            the resultsDateColumnName to set
     */
    public static void setResultsDateColumnName(String resultsDateColumnName) {
        EsacResultsPersistance.resultsDateColumnName = resultsDateColumnName;
    }

    /**
     * @return the resultsPerformanceColumnName
     */
    public static String getResultsPerformanceColumnName() {
        return resultsPerformanceColumnName;
    }

    /**
     * @param resultsPerformanceColumnName
     *            the resultsPerformanceColumnName to set
     */
    public static void setResultsPerformanceColumnName(String resultsPerformanceColumnName) {
        EsacResultsPersistance.resultsPerformanceColumnName = resultsPerformanceColumnName;
    }

    /**
     * @return the resultsUnitColumnName
     */
    public static String getResultsUnitColumnName() {
        return resultsUnitColumnName;
    }

    /**
     * @param resultsUnitColumnName
     *            the resultsUnitColumnName to set
     */
    public static void setResultsUnitColumnName(String resultsUnitColumnName) {
        EsacResultsPersistance.resultsUnitColumnName = resultsUnitColumnName;
    }

    /**
     * @return the resultsMessageColumnName
     */
    public static String getResultsMessageColumnName() {
        return resultsMessageColumnName;
    }

    /**
     * @param resultsMessageColumnName
     *            the resultsMessageColumnName to set
     */
    public static void setResultsMessageColumnName(String resultsMessageColumnName) {
        EsacResultsPersistance.resultsMessageColumnName = resultsMessageColumnName;
    }

    /**
     * @return the resultsTotalFilesColumnName
     */
    public static String getResultsTotalFilesColumnName() {
        return resultsTotalFilesColumnName;
    }

    /**
     * @param resultsTotalFilesColumnName
     *            the resultsTotalFilesColumnName to set
     */
    public static void setResultsTotalFilesColumnName(String resultsTotalFilesColumnName) {
        EsacResultsPersistance.resultsTotalFilesColumnName = resultsTotalFilesColumnName;
    }

    /**
     * @return the resultsSuccessFilesColumnName
     */
    public static String getResultsSuccessFilesColumnName() {
        return resultsSuccessFilesColumnName;
    }

    /**
     * @param resultsSuccessFilesColumnName
     *            the resultsSuccessFilesColumnName to set
     */
    public static void setResultsSuccessFilesColumnName(String resultsSuccessFilesColumnName) {
        EsacResultsPersistance.resultsSuccessFilesColumnName = resultsSuccessFilesColumnName;
    }

    /**
     * @return the resultsElapsedTimeColumnName
     */
    public static String getResultsElapsedTimeColumnName() {
        return resultsElapsedTimeColumnName;
    }

    /**
     * @param resultsElapsedTimeColumnName
     *            the resultsElapsedTimeColumnName to set
     */
    public static void setResultsElapsedTimeColumnName(String resultsElapsedTimeColumnName) {
        EsacResultsPersistance.resultsElapsedTimeColumnName = resultsElapsedTimeColumnName;
    }

    /**
     * @return the resultsBytesColumnName
     */
    public static String getResultsBytesColumnName() {
        return resultsBytesColumnName;
    }

    /**
     * @param resultsBytesColumnName
     *            the resultsBytesColumnName to set
     */
    public static void setResultsBytesColumnName(String resultsBytesColumnName) {
        EsacResultsPersistance.resultsBytesColumnName = resultsBytesColumnName;
    }

    /**
     * @return the resultsThreadsColumnName
     */
    public static String getResultsThreadsColumnName() {
        return resultsThreadsColumnName;
    }

    /**
     * @param resultsThreadsColumnName
     *            the resultsThreadsColumnName to set
     */
    public static void setResultsThreadsColumnName(String resultsThreadsColumnName) {
        EsacResultsPersistance.resultsThreadsColumnName = resultsThreadsColumnName;
    }
}