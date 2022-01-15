
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoInsert {
    public static MongoClient mongoClient;
    public static DBCollection dbcollection;


    public static void createDBCollection() {

        if(mongoClient == null){
            MongoClientURI mongoClientUri = new MongoClientURI("mongodb://admin:abc123@ds059654.mongolab.com:59654/twitterspark");
            mongoClient = new MongoClient(mongoClientUri);
            DB db = mongoClient.getDB(mongoClientUri.getDatabase());
            dbcollection = db.getCollection("nfltweet");
        }
        else
            return;
    }

    public static void insertTweet(BasicDBObject basicdbobject) {
        //System.out.println("A Record has been entered");
        dbcollection.insert(basicdbobject);
    }

}
