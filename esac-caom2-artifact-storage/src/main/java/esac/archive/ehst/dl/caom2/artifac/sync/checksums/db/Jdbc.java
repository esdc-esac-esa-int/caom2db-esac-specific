package esac.archive.ehst.dl.caom2.artifac.sync.checksums.db;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 *
 * @author jduran
 *
 */
public class Jdbc {

	// protected final static String DB_PWD_PROP = "esac.tools.db.password";
	protected static final String DB_URL_PROP = "esac.tools.db.url";
	protected static final String DB_HOST_PROP = "esac.tools.db.dbhost";
	protected static final String DB_PORT_PROP = "esac.tools.db.dbport";
	protected static final String DB_NAME_PROP = "esac.tools.db.dbname";
	protected static final String DB_DRIVER_PROP = "esac.tools.db.driver";
	protected final static String DB_USER_PROP = "esac.tools.db.username";
	private static String dbusername = null;
	private static String dbhost = null;
	private static Integer dbport = null;
	private static String dburl = null;
	private static String dbdriver = null;
	private static String dbname = null;

	protected ComboPooledDataSource cpds;

	public String getOwner() {
		return dbusername;
	}

	public synchronized Connection getConnection() throws SQLException {
		return this.cpds.getConnection();
	}

	protected Jdbc() throws PropertyVetoException, NumberFormatException {
		setDbusername(ConfigProperties.getInstance().getProperty(DB_USER_PROP));
		setDburl(ConfigProperties.getInstance().getProperty(DB_URL_PROP));
		setDbdriver(ConfigProperties.getInstance().getProperty(DB_DRIVER_PROP));
		setDbhost(ConfigProperties.getInstance().getProperty(DB_HOST_PROP));
		setDbport(Integer.parseInt(ConfigProperties.getInstance().getProperty(DB_PORT_PROP)));
		setDbname(ConfigProperties.getInstance().getProperty(DB_NAME_PROP));

		String driver = dbdriver;
		String owner = dbusername;
		String password = ConfigProperties.getInstance().getDbPass();
		String url = dburl;

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

	public String getDbusername() {
		return dbusername;
	}

	private void setDbusername(String dbusername) {
		Jdbc.dbusername = dbusername;
	}

	public String getDburl() {
		return dburl;
	}

	private void setDburl(String dburl) {
		Jdbc.dburl = dburl;
	}

	public String getDbdriver() {
		return dbdriver;
	}

	private void setDbdriver(String dbdriver) {
		Jdbc.dbdriver = dbdriver;
	}

	public String getDbhost() {
		return dbhost;
	}

	private void setDbhost(String dbhost) {
		Jdbc.dbhost = dbhost;
	}

	public String getDbname() {
		return dbname;
	}

	private void setDbname(String dbname) {
		Jdbc.dbname = dbname;
	}

	public Integer getDbport() {
		return dbport;
	}

	private void setDbport(Integer dbport) {
		Jdbc.dbport = dbport;
	}
}