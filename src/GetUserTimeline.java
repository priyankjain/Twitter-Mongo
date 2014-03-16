

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.json.DataObjectFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

/**
 * @author Yusuke Yamamoto - yusuke at mac.com
 * @since Twitter4J 2.1.7
 */
public class GetUserTimeline {
    /**
     * Usage: java twitter4j.examples.timeline.GetUserTimeline
     *
     * @param args String[]
     */

    private static void storeJSON(String rawJSON, String fileName) throws IOException {
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            fos = new FileOutputStream(fileName);
            osw = new OutputStreamWriter(fos, "UTF-8");
            bw = new BufferedWriter(osw);
            bw.append(rawJSON);
            
            bw.flush();
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException ignore) {
                }
            }
            if (osw != null) {
                try {
                    osw.close();
                } catch (IOException ignore) {
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ignore) {
                }
            }
        }
    }
	
    public static void main(String[] args)throws IOException {
        // gets Twitter instance with default credentials
        Twitter twitter = new TwitterFactory().getInstance();
        try {
           // ArrayList<Status> tweets= new ArrayList<Status>();
            String user="BiggBoss";
            ResponseList<Status> statuses;
            File f=new File("BiggBoss.json");
            String rawJSON="";
            Paging pg=new Paging();
            long lastID=Long.MAX_VALUE;	
            Mongo mongo=new Mongo();
            DB db=mongo.getDB("tvshows");
            do
            {
            	statuses=twitter.getUserTimeline(user,pg);
            	for(Status tweet:statuses)
            	{
            		if(tweet.getId()<lastID)
            			lastID=tweet.getId();
            		BasicDBObject object=(BasicDBObject)JSON.parse(DataObjectFactory.getRawJSON(tweet));
            		db.getCollection("bb7").insert(object);
            	}
            	pg.maxId(lastID-1);
            	
            }
            while(statuses.size()!=0);
         
            storeJSON(rawJSON, "BiggBoss.json");
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get timeline: " + te.getMessage());
            System.exit(-1);
        }
    }
}
