package esac.archive.ehst.dl.caom2.repo.client.publications.db;

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
    private String adsAuth;

    private Boolean initiated = false;

    public void init(String connection, String driver, String database, String schema, String host, Integer port, String username, String password,
            String adsUrl, String adsToken, String adsAuth) {
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
        this.setAdsAuth(adsAuth);
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
    public String getAdsAuth() {
        return adsAuth;
    }
    public void setAdsAuth(String adsAuth) {
        this.adsAuth = adsAuth;
    }
}
