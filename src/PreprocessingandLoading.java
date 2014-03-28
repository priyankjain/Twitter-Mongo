import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

//import twitter4j.JSONObject;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class PreprocessingandLoading extends Thread{

	Mongo mongo;
	public void run()
	{
		try
		{
			mongo = new Mongo();
		while(true)
		{
			if(!TweetQueue.tweetQueue.isEmpty())
			{
				
				String tweet=TweetQueue.tweetQueue.poll();
				JSONObject obj_tweet=new JSONObject(tweet);
				obj_tweet.remove("id");
				String text="";
				//Retweet
				JSONObject obj_user;
				String user_id;
				if(obj_tweet.has("retweeted_status"))
				{
					JSONObject retweeted_obj_tweet=obj_tweet.getJSONObject("retweeted_status");
					text=retweeted_obj_tweet.getString("text");
					retweeted_obj_tweet.remove("id");
					String retweet_id=retweeted_obj_tweet.getString("id_str");
					int mainSentiment;
					if(text.length()!=0)
					{
						mainSentiment = SentimentAnalyzer.findSentiment(text);					
					}
					else
					{
						mainSentiment=-1;
					}					
					obj_tweet.put("sentiment", mainSentiment);
					obj_tweet.remove("retweeted_status");
					obj_tweet.put("retweet_id",retweet_id);
					/*
					 * Find the reference doc_id of the original tweet in tweet table using retweeted_obj_tweet.getString("id_str");
					 * obj_tweet.put("retweeted_id", doc_id);
					 */										
					obj_user=obj_tweet.getJSONObject("user");
					obj_tweet.remove("user");
					user_id=obj_user.getString("id_str");				
					obj_tweet.put("user_id",user_id);
					/*
					 * 1) Insert the JSONObject obj_tweet in tweet table
					 * 
					 * 2) Check if id_str exists in the user table, 
					 * if it does not then,
					 * just insert the JSONObject obj_user in user table
					 * 
					 * obj_tweet.toString() and obj_user.toString() for conversion
					 */					
				}
				//Normal tweet
				else					
				{
				text=obj_tweet.getString("text");
				int mainSentiment;
				if(text.length()!=0)
				{
					mainSentiment = SentimentAnalyzer.findSentiment(text);					
				}
				else
				{
					mainSentiment=-1;
				}					
				obj_tweet.put("sentiment", mainSentiment);
				obj_tweet.put("retweet_id","null");
				obj_user=obj_tweet.getJSONObject("user");
				obj_tweet.remove("user");
				user_id=obj_user.getString("id_str");				
				obj_tweet.put("user_id", user_id);
				/*
				 * 1) Insert the JSONObject obj_tweet in tweet table
				 * 
				 * 2) Check if id_str exists in the user table, 
				 * if it does not then,
				 * just insert the JSONObject obj_user in user table
				 * 
				 * obj_tweet.toString() and obj_user.toString() for conversion
				 */					
				}
				
				String name=obj_user.getString("name");
				if(name.length()!=0)
				{
					name=name.split(" ")[0];
				}
				JSONObject json = JsonReader.readJsonFromUrl("http://api.genderize.io/?name="+name);
				obj_user.put("gender", json.get("gender"));
				DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Date date = new Date();
				String datestring=dateFormat.format(date);
				obj_user.remove("id");
				obj_tweet.put("inserted_at",datestring);
				obj_user.put("inserted_at",datestring);
				String tweet_id=obj_tweet.getString("id_str");
				try
				{
				insertIntoMongo(obj_tweet.toString(),obj_user.toString(),tweet_id,user_id);
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void insertIntoMongo(String tweet,String user,String tweet_id,String user_id)throws UnknownHostException
	{
		DB db=mongo.getDB("tvshows");
		DBCollection tweetCollection=db.getCollection("tweet");
		BasicDBObject tweetquery=new BasicDBObject("id_str",tweet_id);
		if(tweetCollection.find(tweetquery).count()==0)
		{
			DBObject tweetObject=(DBObject)JSON.parse(tweet);
			tweetCollection.insert(tweetObject);
		}
		DBCollection userCollection=db.getCollection("user");
		BasicDBObject query=new BasicDBObject("id_str",user_id);
		if(userCollection.find(query).count()==0)
		{
			DBObject userObject=(DBObject)JSON.parse(tweet);
			userCollection.insert(userObject);
		}
	}
}
