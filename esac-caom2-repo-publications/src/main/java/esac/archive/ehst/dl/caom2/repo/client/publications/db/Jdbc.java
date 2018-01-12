package esac.archive.ehst.dl.caom2.repo.client.publications.db;

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

    protected ComboPooledDataSource cpds;

    public synchronized Connection getConnection() throws SQLException {
        return this.cpds.getConnection();
    }

    protected Jdbc() throws PropertyVetoException, NumberFormatException {
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass(ConfigProperties.getInstance().getDriver());
        cpds.setJdbcUrl(ConfigProperties.getInstance().getConnection());
        cpds.setUser(ConfigProperties.getInstance().getUsername());
        cpds.setPassword(ConfigProperties.getInstance().getPassword());

        cpds.setMinPoolSize(5);
        cpds.setAcquireIncrement(5);
        cpds.setMaxPoolSize(50);
    }
}