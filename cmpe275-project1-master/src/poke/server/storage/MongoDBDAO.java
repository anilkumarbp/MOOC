package poke.server.storage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;

import poke.server.conf.MongoDBConfiguration;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MongoDBDAO  {


private static DB dbDAO;
private static DBCollection collDAO;
private String dbHostName;
private String dbHostName2;
private String dbHostName3;
private MongoClient mongoClientDAO;

    int dbPortNumber;
    int dbPortNumber2;
    int dbPortNumber3;
    String dbName;
    String dbUserName;
    String dbPassword;
 
       

public MongoDBDAO() throws FileNotFoundException, IOException {
	MongoDBConfiguration mgDBConf = new MongoDBConfiguration(); 
	this.dbHostName = MongoDBConfiguration.getDbHost();
	this.dbHostName2 = MongoDBConfiguration.getDbHost2();
	this.dbHostName3 = MongoDBConfiguration.getDbHost3();
	this.dbPortNumber = MongoDBConfiguration.getDbPort();
	this.dbPortNumber2 = MongoDBConfiguration.getDbPort2();
	this.dbPortNumber3 = MongoDBConfiguration.getDbPort3();
	this.dbName = "moocdata";
	this.dbUserName = "";
	this.dbPassword = "";
	}

public MongoClient getDBConnection() throws UnknownHostException {
	mongoClientDAO = new MongoClient(Arrays.asList(new ServerAddress(dbHostName, dbPortNumber),
            new ServerAddress(dbHostName2, dbPortNumber2),
            new ServerAddress(dbHostName3, dbPortNumber3)));
	return mongoClientDAO;
	}

public DB getDB (String dbName)	{
dbDAO = mongoClientDAO.getDB(dbName);
return dbDAO;
}

public DBCollection getCollection(String collection) {
collDAO = dbDAO.getCollection(collection);
return collDAO;
}

public void insertData(BasicDBObject doc)	{
collDAO.insert(doc);
}

public void deleteData(BasicDBObject doc)	{
collDAO.findAndRemove(doc);
}

public void updateData(BasicDBObject query, BasicDBObject update )	{
collDAO.findAndModify(query, update);
}


public void closeConnection() {
mongoClientDAO.close();

}

public DBCursor findData(BasicDBObject query1) {
return collDAO.find(query1);
}

public String getDbHostName() {
return dbHostName;
}

public void setDbHostName(String dbHostName) {
this.dbHostName = dbHostName;
}

public int getDbPortNumber() {
return dbPortNumber;
}

public void setDbPortNumber(int dbPortNumber) {
this.dbPortNumber = dbPortNumber;
}

public String getDbName() {
return dbName;
}

public void setDbName(String dbName) {
this.dbName = dbName;
}

public String getDbUserName() {
return dbUserName;
}

public void setDbUserName(String dbUserName) {
this.dbUserName = dbUserName;
}

public String getDbPassword() {
return dbPassword;
}

public void setDbPassword(String dbPassword) {
this.dbPassword = dbPassword;
}

}