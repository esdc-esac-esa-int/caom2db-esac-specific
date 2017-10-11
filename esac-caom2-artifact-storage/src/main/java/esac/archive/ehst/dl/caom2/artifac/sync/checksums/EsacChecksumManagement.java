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
public class EsacChecksumManagement {

	private static EsacChecksumManagement instance = null;

	private static Logger log = Logger.getLogger(EsacChecksumManagement.class.getName());

	protected static final String SCHEMA = "caom2.artifactsync.checksumTable.schema";
	protected static final String TABLENAME = "caom2.artifactsync.checksumTable.table";
	protected static final String COLUMN_ARTIFACT = "caom2.artifactsync.checksumTable.columnArtifact";
	protected static final String COLUMN_CHECKSUM = "caom2.artifactsync.checksumTable.columnChecksum";
	protected static final String FILES_LOCATION = "caom2.artifactsync.repository.root";
	private static String checksumSchema = null;
	private static String checksumTable = null;
	private static String checksumArtifactColumnName = null;
	private static String checksumChecksumColumnName = null;
	private static String filesLocation = null;

	public static EsacChecksumManagement getInstance() {
		if (instance == null) {
			instance = new EsacChecksumManagement();
		}
		return instance;
	}

	private EsacChecksumManagement() {

		try {

			EsacChecksumManagement.checksumSchema = ConfigProperties.getInstance().getProperty(SCHEMA);
			EsacChecksumManagement.checksumTable = ConfigProperties.getInstance().getProperty(TABLENAME);
			EsacChecksumManagement.checksumArtifactColumnName = ConfigProperties.getInstance()
					.getProperty(COLUMN_ARTIFACT);
			EsacChecksumManagement.checksumChecksumColumnName = ConfigProperties.getInstance()
					.getProperty(COLUMN_CHECKSUM);

			EsacChecksumManagement.filesLocation = ConfigProperties.getInstance().getProperty(FILES_LOCATION);

			boolean tableExists = checkIfTableExists(checksumSchema, checksumTable);
			if (!tableExists) {
				throw new Exception("Table " + checksumSchema + "." + checksumTable
						+ " doesn't exist. Check configuration file and permissions in database");
			}

			// preparedStatement =
			// createPreparedStatement(JdbcSingleton.getInstance().getConnection(),
			// checksumSchema,
			// checksumTable);

		} catch (Exception e) {
			log.error("Unexpected exception constructing EsacChecksumManagement: " + e.getMessage());
			System.exit(1);
		}
	}

	public static String getFilesLocation() {
		return filesLocation;
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

	public boolean upsert(URI artifactURI, URI checksum) {
		// INSERT INTO tablename (a, b, c) values (1, 2, 10) ON CONFLICT (a) DO
		// UPDATE SET c = tablename.c + 1;
		boolean result = false;
		String upsert = null;
		if (checksum != null) {
			upsert = "insert into " + checksumSchema + "." + checksumTable + " (" + checksumArtifactColumnName + ", "
					+ checksumChecksumColumnName + ") " + " values ('" + artifactURI.toString() + "', '"
					+ checksum.toString() + "') on conflict (" + checksumArtifactColumnName + ") do update set "
					+ checksumChecksumColumnName + " = '" + checksum.toString() + "';";
		} else {
			upsert = "insert into " + checksumSchema + "." + checksumTable + " (" + checksumArtifactColumnName + ", "
					+ checksumChecksumColumnName + ") " + " values ('" + artifactURI.toString()
					+ "', NULL) on conflict (" + checksumArtifactColumnName + ") do update set "
					+ checksumChecksumColumnName + " = NULL;";
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

	// private static PreparedStatement createPreparedStatement(Connection con,
	// String schema, String tableName)
	// throws IllegalArgumentException, SecurityException,
	// IllegalAccessException, InvocationTargetException,
	// NoSuchMethodException, SQLException {
	// PreparedStatement insertValues = null;
	//
	// String table = schema + "." + tableName;
	//
	// String columns = checksumArtifactColumnName + "," +
	// checksumChecksumColumnName;
	// String values = "?,?";
	//
	// String insertStatement = "insert into " + table + "(" + columns + ")" + "
	// values (" + values + ");";
	//
	// log.debug("Insertion prepared statement " + insertStatement);
	//
	// insertValues = con.prepareStatement(insertStatement);
	//
	// return insertValues;
	// }

	// private static int databaseWriter(PreparedStatement insertValues,
	// List<EsacChecksumPersistance> sources)
	// throws SQLException {
	// int idx = 1;
	// for (EsacChecksumPersistance source : sources) {
	// insertValues.setString(idx++, source.getArtifactURI().toString());
	// if (source.getChecksum() != null) {
	// insertValues.setString(idx++, source.getChecksum().toString());
	// } else {
	// insertValues.setNull(idx++, java.sql.Types.VARCHAR);
	// }
	// }
	// insertValues.addBatch();
	// insertValues.executeBatch();
	//
	// return sources.size();
	// }

	public boolean select(URI artifactURI) {
		boolean result = false;
		Statement stmt = null;
		String query = "select * from " + checksumSchema + "." + checksumTable + " where " + checksumArtifactColumnName
				+ " = '" + artifactURI.toString() + "';";
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
		String query = "select * from " + checksumSchema + "." + checksumTable + " where " + checksumArtifactColumnName
				+ " = '" + artifactURI.toString() + "' and " + checksumChecksumColumnName + " = '" + checksum.toString()
				+ "'";
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

	public void insert(URI artifactURI, URI checksum) {
		String insert = null;
		if (checksum != null) {
			insert = "insert into " + checksumSchema + "." + checksumTable + " (" + checksumArtifactColumnName + ", "
					+ checksumChecksumColumnName + ") " + " values ('" + artifactURI.toString() + "', '"
					+ checksum.toString() + "');";
		} else {
			insert = "insert into " + checksumSchema + "." + checksumTable + " (" + checksumArtifactColumnName + ") "
					+ " values ('" + artifactURI.toString() + "');";
		}
		log.debug("insert = " + insert);

		Connection con = null;
		Statement stmt = null;
		try {
			con = JdbcSingleton.getInstance().getConnection();
			stmt = con.createStatement();
			int res = stmt.executeUpdate(insert);
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
	}

	public void update(URI artifactURI, URI checksum) {
		String update = null;
		if (checksum != null) {
			update = "update " + checksumSchema + "." + checksumTable + " set " + checksumChecksumColumnName + " = '"
					+ checksum.toString() + "' where " + checksumArtifactColumnName + " = '" + artifactURI.toString()
					+ "';";
		} else {
			update = "update " + checksumSchema + "." + checksumTable + " set " + checksumChecksumColumnName + " = "
					+ null + " where " + checksumArtifactColumnName + " = '" + artifactURI.toString() + "';";
		}
		log.debug("update = " + update);

		Connection con = null;
		Statement stmt = null;
		try {
			con = JdbcSingleton.getInstance().getConnection();
			stmt = con.createStatement();
			int res = stmt.executeUpdate(update);
			if (res != 1) {
				throw new RuntimeException("Unexpected exception updating artifact: " + artifactURI.toString());
			}
		} catch (Exception ex) {
			throw new RuntimeException("Unexpected exception when updating: " + ex.getMessage());
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
	}
}
