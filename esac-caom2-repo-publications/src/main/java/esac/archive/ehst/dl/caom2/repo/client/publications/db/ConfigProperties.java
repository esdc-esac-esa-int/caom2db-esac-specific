package esac.archive.ehst.dl.caom2.repo.client.publications.db;

import org.hibernate.Session;
import org.hibernate.SessionFactory;

public class ConfigProperties {

    private static ConfigProperties instance = null;
    public static ConfigProperties getInstance() {
        if (instance == null) {
            instance = new ConfigProperties();
        }
        return instance;
    }
    private ConfigProperties() {

    }

    private String connection;
    private String driver;
    private String database;
    private String schema;
    private String host;
    private Integer port;
    private String username;
    private String password;
    private String adsUrl;
    private String adsToken;
    private String resource;
    private Integer nThreads;
    private Session session;
    private SessionFactory factory;
    private boolean isLocal = false;
    private Integer connsToADS = null;

    private Boolean initiated = false;

    public void init(String connection, String driver, String database, String schema, String host, Integer port, String username, String password,
            String adsUrl, String adsToken, String resource, Integer nThreads, Integer connsToADS, SessionFactory factory, boolean isLocal) {
        this.setConnection(connection + ":" + port + "/" + database);
        this.setDriver(driver);
        this.setDatabase(database);
        this.setSchema(schema);
        this.setHost(host);
        this.setPort(port);
        this.setUsername(username);
        this.setPassword(password);
        this.setAdsUrl(adsUrl);
        this.setAdsToken(adsToken);
        this.setResource(resource);
        this.setnThreads(nThreads);
        this.setFactory(factory);
        this.setLocal(isLocal);
        this.setConnsToADS(connsToADS);
        //        this.setSession(session);
        initiated = true;
    }
    public String getConnection() {
        return connection;
    }
    public void setConnection(String connection) {
        this.connection = connection;
    }
    public String getDatabase() {
        return database;
    }
    public void setDatabase(String database) {
        this.database = database;
    }
    public String getSchema() {
        return schema;
    }
    public void setSchema(String schema) {
        this.schema = schema;
    }
    public String getHost() {
        return host;
    }
    public void setHost(String host) {
        this.host = host;
    }
    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }
    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }
    public String getDriver() {
        return driver;
    }
    public void setDriver(String driver) {
        this.driver = driver;
    }
    public Integer getPort() {
        return port;
    }
    public void setPort(Integer port) {
        this.port = port;
    }
    public String getAdsUrl() {
        return adsUrl;
    }
    public void setAdsUrl(String adsUrl) {
        this.adsUrl = adsUrl;
    }
    public String getAdsToken() {
        return adsToken;
    }
    public void setAdsToken(String adsToken) {
        this.adsToken = adsToken;
    }
    public String getResource() {
        return resource;
    }
    public void setResource(String resource) {
        this.resource = resource;
    }
    public Integer getnThreads() {
        return nThreads;
    }
    public void setnThreads(Integer nThreads) {
        this.nThreads = nThreads;
    }
    public SessionFactory getFactory() {
        return factory;
    }
    public void setFactory(SessionFactory factory) {
        this.factory = factory;
    }
    //    public Session getSession() {
    //        return session;
    //    }
    //    public void setSession(Session session) {
    //        this.session = session;
    //    }
    public boolean isLocal() {
        return isLocal;
    }
    public void setLocal(boolean isLocal) {
        this.isLocal = isLocal;
    }
    public Integer getConnsToADS() {
        return connsToADS;
    }
    public void setConnsToADS(Integer connsToADS) {
        this.connsToADS = connsToADS;
    }
}
