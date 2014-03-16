import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import twitter4j.JSONException;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.json.DataObjectFactory;

public final class RateLimitedSearch {

    private static void storeJSON(String rawJSON, String fileName) throws IOException {
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            fos = new FileOutputStream(fileName);
            osw = new OutputStreamWriter(fos, "UTF-8");
            bw = new BufferedWriter(osw);
            bw.write(rawJSON);
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
	
	public static void main(String[] args) throws TwitterException,
			FileNotFoundException, IOException, SAXException, InterruptedException, TransformerException,
			ParserConfigurationException,JSONException {
		PreprocessingandLoading thread=new PreprocessingandLoading();
		thread.start();
		File fXmlFile = new File("xml/shows.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		NodeList nList = doc.getElementsByTagName("show");
		Twitter twitter = new TwitterFactory().getSingleton();
		Properties prop = new Properties();
		prop.load(new FileInputStream("twitter4j.properties"));
		int count=0,show_no=0;
		int no_of_shows=nList.getLength();
		while(true)
		{
			RateLimitStatus rls= twitter.getRateLimitStatus().get("/users/search");;
			if(rls.getRemaining()<30)
				{
					Thread.sleep(rls.getSecondsUntilReset()*1000);
					continue;
				}
			for(int i=count;i-count<30;i++)
			{	
				// For each show
				show_no=count%(no_of_shows);
				count++;
				Node nNode = nList.item(show_no);
				Element show = (Element) nNode;
				String tagList[]=show.getElementsByTagName("tags").item(0).getTextContent().split(",");
				String since= show.getElementsByTagName("since").item(0).getTextContent();
				String q="";
				for(String tag: tagList)
				{
				q+=tag+" OR ";
				}
				q=q.substring(0, q.length()-4);
				Query query=new Query(q);
				query.setCount(5000);
				query.setSinceId(Long.parseLong(since));
				QueryResult result;
				result = twitter.search(query);
				List<Status> tweets = result.getTweets();
				try
				{
				for (Status tweet : tweets) 
					{
					String rawJSON = DataObjectFactory.getRawJSON(tweet);
					JSONObject obj_tweet=new JSONObject(rawJSON);
					obj_tweet.put("show_name",show.getElementsByTagName("name").item(0).getTextContent());
					String tweet_text=obj_tweet.getString("text");
					String keywords="";
					for(int j=0;j<tagList.length;j++)
					{
						if(tweet_text.toLowerCase().contains(tagList[j].toLowerCase()))
							{
								keywords+=tagList[j]+",";
							}
						
					}
					
					obj_tweet.put("keywords", keywords);
					rawJSON=obj_tweet.toString();
					storeJSON(rawJSON+"\n\n","show.txt");
					TweetQueue.tweetQueue.add(rawJSON);
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				if(tweets.size()>1)
				since=String.valueOf(tweets.get(tweets.size()-1).getId());
				else 
				{
					System.out.println("Sleeping now for 15 minutes....now new tweets retrieved");
					Thread.sleep(15*60*1000);
				}
				show.getElementsByTagName("since").item(0).setTextContent(since);
				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer transformer = transformerFactory.newTransformer();
				DOMSource domSource = new DOMSource(doc);
				StreamResult streamResult = new StreamResult(fXmlFile);
				transformer.transform(domSource, streamResult);
			}
		}	
	}
}
