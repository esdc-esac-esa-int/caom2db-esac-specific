package esac.archive.caom2.artifact.sync.db;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import esac.archive.caom2.artifact.sync.ConfigProperties;

/**
 *
 * @author jduran
 * @author Raul Gutierrez-Sanchez Copyright (c) 2014- European Space Agency
 *
 */
public class JdbcSingleton {


	private String dbUsername = null;
	private String dbPassword = null;
	private String dbHost = null;
	private Integer dbPort = null;
	private String dbDriver = null;
	private String dbName = null;
	private String dbSchema = null;

	protected ComboPooledDataSource cpds;

	public synchronized Connection getConnection() throws SQLException {
		return this.cpds.getConnection();
	}
	
    protected JdbcSingleton() throws PropertyVetoException {
        setDbUsername(ConfigProperties.getInstance().getProperty(ConfigProperties.PROP_DB_USER));
        setDbPassword(ConfigProperties.getInstance().getProperty(ConfigProperties.PROP_DB_PASSWORD));
        setDbDriver(ConfigProperties.getInstance().getProperty(ConfigProperties.PROP_DB_DRIVER));
        setDbHost(ConfigProperties.getInstance().getProperty(ConfigProperties.PROP_DB_HOST));
        setDbPort(Integer.parseInt(ConfigProperties.getInstance().getProperty(ConfigProperties.PROP_DB_PORT)));
        setDbName(ConfigProperties.getInstance().getProperty(ConfigProperties.PROP_DB_NAME));
        setDbSchema(ConfigProperties.getInstance().getProperty(ConfigProperties.PROP_DB_SCHEMA));

        cpds = new ComboPooledDataSource();
        cpds.setDriverClass(getDbDriver()); // loads the jdbc driver
        cpds.setJdbcUrl(getDbUrl());
        cpds.setUser(getDbUsername());
        cpds.setPassword(getDbPassword());

        // the settings below are optional -- c3p0 can work with defaults
        cpds.setMinPoolSize(5);
        cpds.setAcquireIncrement(5);
        cpds.setMaxPoolSize(50);
    }

    private static JdbcSingleton instance = null;

    private synchronized static void createInstance() throws PropertyVetoException {
        if (instance == null) {
            instance = new JdbcSingleton();
        }
    }

    public static JdbcSingleton getInstance() throws PropertyVetoException {
        createInstance();
        return instance;
    }

	public String getDbUsername() {
		return dbUsername;
	}

	public void setDbUsername(String dbUsername) {
		this.dbUsername = dbUsername;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	public String getDbHost() {
		return dbHost;
	}

	public void setDbHost(String dbHost) {
		this.dbHost = dbHost;
	}

	public Integer getDbPort() {
		return dbPort;
	}

	public void setDbPort(Integer dbPort) {
		this.dbPort = dbPort;
	}

	public String getDbDriver() {
		return dbDriver;
	}

	public void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDbSchema() {
		return dbSchema;
	}

	public void setDbSchema(String dbSchema) {
		this.dbSchema = dbSchema;
	}
	
	public String getDbUrl() {
		return "jdbc:postgresql://" + getDbHost() + ":" + getDbPort() + "/" + getDbName();
	}
	
    public String getOwner() {
        return dbUsername;
    }
}