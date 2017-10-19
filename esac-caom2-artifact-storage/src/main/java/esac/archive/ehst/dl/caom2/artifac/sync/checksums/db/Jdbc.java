package esac.archive.ehst.dl.caom2.artifac.sync.checksums.db;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import esac.archive.ehst.dl.caom2.artifac.sync.checksums.EsacChecksumPersistance;

/**
 *
 * @author jduran
 *
 */
public class Jdbc {

	// protected final static String DB_PWD_PROP = "esac.tools.db.password";

	protected ComboPooledDataSource cpds;

	public String getOwner() {
		return EsacChecksumPersistance.getInstance().getDbusername();
	}

	public synchronized Connection getConnection() throws SQLException {
		return this.cpds.getConnection();
	}

	protected Jdbc() throws PropertyVetoException {

		String driver = EsacChecksumPersistance.getInstance().getDbdriver();
		String owner = EsacChecksumPersistance.getInstance().getDbusername();
		String password = ConfigProperties.getInstance().getDbPass();
		String url = EsacChecksumPersistance.getInstance().getDburl();

		cpds = new ComboPooledDataSource();
		cpds.setDriverClass(driver); // loads the jdbc driver
		cpds.setJdbcUrl(url);
		cpds.setUser(owner);
		cpds.setPassword(password);

		// the settings below are optional -- c3p0 can work with defaults
		cpds.setMinPoolSize(5);
		cpds.setAcquireIncrement(5);
		cpds.setMaxPoolSize(50);

	}

}