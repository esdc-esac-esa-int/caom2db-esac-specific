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

	protected static final String SCRIPT = "caom2.artifactsync.checksumTable.creationScript";
	protected static final String SCHEMA = "caom2.artifactsync.checksumTable.schema";
	protected static final String TABLENAME = "caom2.artifactsync.checksumTable.table";
	protected static final String COLUMN_ARTIFACT = "caom2.artifactsync.checksumTable.columnArtifact";
	protected static final String COLUMN_CHECKSUM = "caom2.artifactsync.checksumTable.columnChecksum";

	private static String checksumCreateScript = null;
	private static String checksumSchema = null;
	private static String checksumTable = null;
	private static String checksumArtifactColumnName = null;
	private static String checksumChecksumColumnName = null;

	private static EsacChecksumPersistance instance = null;

	public static EsacChecksumPersistance getInstance() {
		if (instance == null) {
			instance = new EsacChecksumPersistance();
		}
		return instance;
	}

	private EsacChecksumPersistance() {
		try {

			setChecksumCreateScript(ConfigProperties.getInstance().getProperty(SCRIPT));
			setChecksumSchema(ConfigProperties.getInstance().getProperty(SCHEMA));
			setChecksumTable(ConfigProperties.getInstance().getProperty(TABLENAME));
			setChecksumArtifactColumnName(ConfigProperties.getInstance().getProperty(COLUMN_ARTIFACT));
			setChecksumChecksumColumnName(ConfigProperties.getInstance().getProperty(COLUMN_CHECKSUM));

			boolean tableExists = checkIfTableExists(getChecksumSchema(), getChecksumTable());
			if (!tableExists) {
				createChecksumTable(JdbcSingleton.getInstance().getDbusername(),
						JdbcSingleton.getInstance().getDbname(), JdbcSingleton.getInstance().getDbhost(),
						checksumCreateScript);
				tableExists = checkIfTableExists(getChecksumSchema(), getChecksumTable());
				if (!tableExists) {
					throw new Exception("Table " + getChecksumSchema() + "." + getChecksumTable()
							+ " doesn't exist. Check configuration file and permissions in database");
				}

			}

		} catch (Exception e) {
			log.error("Unexpected exception constructing EsacChecksumPersistance: " + e.getMessage());
			System.exit(1);
		}
	}

	private void createChecksumTable(String user, String dbName, String host, String checksumCreateScript) {
		try {
			String sql = "psql -w -U " + user + " -d " + dbName + " -h " + host + " -f " + checksumCreateScript;
			Process p = Runtime.getRuntime().exec(sql);
			p.waitFor();
		} catch (Exception err) {
			log.error("Unexpected exception creating checksum table: " + err.getMessage());
			err.printStackTrace();
			System.exit(2);
		}
	}

	public boolean upsert(URI artifactURI, URI md5) {
		// INSERT INTO tablename (a, b, c) values (1, 2, 10) ON CONFLICT (a) DO
		// UPDATE SET c = tablename.c + 1;
		boolean result = false;
		String upsert = null;
		if (md5 != null) {
			upsert = "insert into " + getChecksumSchema() + "." + getChecksumTable() + " ("
					+ getChecksumArtifactColumnName() + ", " + getChecksumChecksumColumnName() + ") " + " values ('"
					+ artifactURI.toString() + "', '" + md5.toString() + "') on conflict ("
					+ getChecksumArtifactColumnName() + ") do update set " + getChecksumChecksumColumnName() + " = '"
					+ md5.toString() + "';";
		} else {
			upsert = "insert into " + getChecksumSchema() + "." + getChecksumTable() + " ("
					+ getChecksumArtifactColumnName() + ", " + getChecksumChecksumColumnName() + ") " + " values ('"
					+ artifactURI.toString() + "', NULL) on conflict (" + getChecksumArtifactColumnName()
					+ ") do update set " + getChecksumChecksumColumnName() + " = NULL;";
		}
		log.debug("insert = " + upsert);

		Connection con = null;
		Statement stmt = null;
		try {
			con = JdbcSingleton.getInstance().getConnection();
			stmt = con.createStatement();
			int res = stmt.executeUpdate(upsert);
			if (res != 1) {
				throw new RuntimeException("Unexpected exception inserting artifact: " + artifactURI.toString());
			}
		} catch (Exception ex) {
			throw new RuntimeException("Unexpected exception when inserting: " + ex.getMessage());
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
		String query = "select * from " + getChecksumSchema() + "." + getChecksumTable() + " where "
				+ getChecksumArtifactColumnName() + " = '" + artifactURI.toString() + "';";
		ResultSet rs = null;

		Connection con = null;
		try {
			con = JdbcSingleton.getInstance().getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			result = rs.next();
		} catch (Exception ex) {
			throw new RuntimeException("Unexpected exception when selecting: " + ex.getMessage());
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

	public boolean select(URI artifactURI, URI checksum) {
		boolean result = false;
		Statement stmt = null;
		String query = "select * from " + getChecksumSchema() + "." + getChecksumTable() + " where "
				+ getChecksumArtifactColumnName() + " = '" + artifactURI.toString() + "' and "
				+ getChecksumChecksumColumnName() + " = '" + checksum.toString() + "'";
		ResultSet rs = null;

		Connection con = null;
		try {
			con = JdbcSingleton.getInstance().getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			result = rs.next();
		} catch (Exception ex) {
			throw new RuntimeException("Unexpected exception when selecting: " + ex.getMessage());
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

	private boolean checkIfTableExists(String checksumSchema, String checksumTable)
			throws SQLException, PropertyVetoException {
		String query = "select exists (select 1 from information_schema.tables where table_schema = '" + checksumSchema
				+ "' and table_name = '" + checksumTable + "')";
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
			throw new RuntimeException("Unexpected exception when selecting: " + ex.getMessage());
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

	public String getChecksumCreateScript() {
		return checksumCreateScript;
	}

	private static void setChecksumCreateScript(String checksumCreateScript) {
		EsacChecksumPersistance.checksumCreateScript = checksumCreateScript;
	}
}