package poke.server.conf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.codehaus.jackson.annotate.JsonProperty;


public class MongoDBConfiguration {
	
	 	public MongoDBConfiguration() throws FileNotFoundException, IOException {
         Properties prop = new Properties();
         java.net.URL url = this.getClass().getResource("mongo.properties");
         InputStream inputStream = url.openStream();
         prop.load(inputStream);
         
         dbHost = prop.getProperty("host");
         dbPort = Integer.parseInt(prop.getProperty("port"));
         dbHost2 = prop.getProperty("host2");
         dbPort2 = Integer.parseInt(prop.getProperty("port2"));
         dbHost3 = prop.getProperty("host3");
         dbPort3 = Integer.parseInt(prop.getProperty("port3"));
         databasename = prop.getProperty("databasename");
         username = prop.getProperty("username");
         password = prop.getProperty("password");
         usercollection = prop.getProperty("userCollection");
 }

	@JsonProperty
    private static String dbHost;

    @JsonProperty
    private static int dbPort;
    
    @JsonProperty
    private static String dbHost2;

    @JsonProperty
    private static int dbPort2;
    
    @JsonProperty
    private static String dbHost3;

    @JsonProperty
    private static int dbPort3;
    
    @JsonProperty
    private static String databasename;

    @JsonProperty
    private static String username;
    
    @JsonProperty
    private static String password;
    
    @JsonProperty
    private static String usercollection;

	public static String getDbHost() {
		return dbHost;
	}

	public static void setDbHost(String dbHost) {
		MongoDBConfiguration.dbHost = dbHost;
	}

	public static int getDbPort() {
		return dbPort;
	}

	public static void setDbPort(int dbPort) {
		MongoDBConfiguration.dbPort = dbPort;
	}

	public static String getDbHost2() {
		return dbHost2;
	}

	public static void setDbHost2(String dbHost2) {
		MongoDBConfiguration.dbHost2 = dbHost2;
	}

	public static int getDbPort2() {
		return dbPort2;
	}

	public static void setDbPort2(int dbPort2) {
		MongoDBConfiguration.dbPort2 = dbPort2;
	}

	public static String getDbHost3() {
		return dbHost3;
	}

	public static void setDbHost3(String dbHost3) {
		MongoDBConfiguration.dbHost3 = dbHost3;
	}

	public static int getDbPort3() {
		return dbPort3;
	}

	public static void setDbPort3(int dbPort3) {
		MongoDBConfiguration.dbPort3 = dbPort3;
	}

	public static String getDatabasename() {
		return databasename;
	}

	public static void setDatabasename(String databasename) {
		MongoDBConfiguration.databasename = databasename;
	}

	public static String getUsername() {
		return username;
	}

	public static void setUsername(String username) {
		MongoDBConfiguration.username = username;
	}

	public static String getPassword() {
		return password;
	}

	public static void setPassword(String password) {
		MongoDBConfiguration.password = password;
	}

	public static String getUsercollection() {
		return usercollection;
	}

	public static void setUsercollection(String usercollection) {
		MongoDBConfiguration.usercollection = usercollection;
	}
    
	
}
