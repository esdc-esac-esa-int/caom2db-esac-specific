package esac.archive.ehst.dl.caom2.artifac.sync.checksums;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import esac.archive.ehst.dl.caom2.artifac.sync.checksums.db.JdbcSingleton;

/**
 *
 * @author jduran
 *
 */
public class EsacChecksumManagement {

	private static EsacChecksumManagement instance = null;

	private static Logger log = Logger.getLogger(EsacChecksumManagement.class.getName());

	public static EsacChecksumManagement getInstance() throws Exception {
		if (instance == null) {
			instance = new EsacChecksumManagement();
		}
		return instance;
	}

	private EsacChecksumManagement() throws Exception {
		boolean tableExists = checkIfTableExists(EsacChecksumPersistance.getInstance().getChecksumSchema(),
				EsacChecksumPersistance.getInstance().getChecksumTable());
		if (!tableExists) {
			throw new Exception("Table " + EsacChecksumPersistance.getInstance().getChecksumSchema() + "."
					+ EsacChecksumPersistance.getInstance().getChecksumTable()
					+ " doesn't exist. Check configuration file and permissions in database");
		}

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
}
