import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		SimpleDateFormat sdf = new SimpleDateFormat("EE MMM dd HH:mm:ss z yyyy");
		File fXmlFile = new File("xml/shows.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		NodeList nList = doc.getElementsByTagName("show");
		NodeList charSearchList=doc.getElementsByTagName("character");
		Twitter twitter = new TwitterFactory().getSingleton();
		Properties prop = new Properties();
		prop.load(new FileInputStream("twitter4j.properties"));
		int count=0,show_no=0,char_no=0;
		int no_of_shows=nList.getLength();
		int no_of_chars=charSearchList.getLength();
		HashMap showMap=new HashMap();
		HashMap charMap=new HashMap();
		boolean isChar=false;
		int num;
		for(int i=0;i<nList.getLength();i++)
		{
			Element show=(Element)nList.item(i);
			String showName=((show.getElementsByTagName("name").item(0)).getTextContent()).toLowerCase();
			String tags[]=show.getElementsByTagName("tags").item(0).getTextContent().split(",");
			for(String tag:tags)
			{
				showMap.put(tag,showName);
			}	
			NodeList characters=show.getElementsByTagName("characters");
			for(int j=0;j<characters.getLength();j++)
			{
				String character=((((Element)characters.item(j)).getElementsByTagName("title").item(0)).getTextContent()).toLowerCase();
				String keys[]=((((Element)characters.item(j)).getElementsByTagName("keywords").item(0)).getTextContent()).toLowerCase().split(",");
				for(String key:keys)
				{
					charMap.put(key,character+","+showName);
				}
			}
		}
		while(true)
		{
			RateLimitStatus rls= twitter.getRateLimitStatus().get("/search/tweets");
			if(rls.getRemaining()<10)
				{
					int seconds=rls.getSecondsUntilReset();
					System.out.println("Now sleeping for "+seconds+" seconds");
					Thread.sleep(seconds*1000);
					continue;
				}
			for(int i=0;i<10;i++)
			{	
				// For each show
				num=count%(no_of_shows+no_of_chars);
				count++;
				count%=(no_of_shows+no_of_chars);
				Node nNode;
				String tagList[];
				String since;
				Element show=null;
				Element character=null;
				if(num<no_of_shows)
				{//Search for show
					isChar=false;
					show_no=num;
					nNode = nList.item(show_no);
					show=(Element)nNode;
					tagList=show.getElementsByTagName("tags").item(0).getTextContent().split(",");
					since= show.getElementsByTagName("since").item(0).getTextContent();
				}
				else
				{
					isChar=true;
					char_no=num-no_of_shows;
					nNode=charSearchList.item(char_no);
					character=(Element)nNode;
					System.out.println("Now processing character "+character.getElementsByTagName("title").item(0).getTextContent());
					tagList=character.getElementsByTagName("keywords").item(0).getTextContent().split(",");
					since=character.getElementsByTagName("since").item(0).getTextContent();
				}
				String q="";
				for(String tag: tagList)
				{
				q+=tag+" OR ";
				}
				q=q.substring(0, q.length()-4);
				Query query=new Query(q);
				query.setCount(100);
				query.setSinceId(Long.parseLong(since));
				QueryResult result=null;
				List<Status> tweets=null;
				int iter=0;
				do
				{
					try
					{
						System.out.println("iteration number "+iter);
						iter++;
						if(iter%10==0)
						{
							rls= twitter.getRateLimitStatus().get("/search/tweets");
							if(rls.getRemaining()<10)
							{
								int seconds=rls.getSecondsUntilReset();
								System.out.println("Now sleeping for "+seconds+" seconds");
								Thread.sleep(seconds*1000);
								continue;
							}
						}
						result = twitter.search(query);
						tweets = result.getTweets();
					for (Status tweet : tweets) 
						{
						String rawJSON = DataObjectFactory.getRawJSON(tweet);
						JSONObject obj_tweet=new JSONObject(rawJSON);
						//obj_tweet.put("show_name",show.getElementsByTagName("name").item(0).getTextContent());
						String tweet_text=obj_tweet.getString("text");
						String keywords="";
						String show_list="";
						String character_list="";
						if(!isChar)
						show_list=show.getElementsByTagName("name").item(0).getTextContent().toLowerCase()+",";
						else
						{
							character_list=character.getElementsByTagName("title").item(0).getTextContent().toLowerCase()+",";
							Element parent_show=(Element)character.getParentNode().getParentNode();
							String parent_show_name=parent_show.getElementsByTagName("name").item(0).getTextContent().toLowerCase();
							if(!show_list.contains(parent_show_name))
							{
								show_list+=parent_show_name+",";
							}
						}
						for(int j=0;j<tagList.length;j++)
						{
							if(tweet_text.toLowerCase().contains(tagList[j].toLowerCase()))
								{
									keywords+=tagList[j].toLowerCase()+",";
								}	
						}
						Iterator it=showMap.entrySet().iterator();
						while(it.hasNext())
						{
							Map.Entry me=(Map.Entry)it.next();
							if(tweet_text.toLowerCase().contains((String)me.getKey()) && !show_list.contains((String)me.getValue()))
							{
								show_list+=me.getValue()+",";
							}
						}
						it=charMap.entrySet().iterator();
						while(it.hasNext())
						{
							Map.Entry me=(Map.Entry)it.next();
							String show_for_character[]=((String)me.getValue()).split(",");
							if(tweet_text.toLowerCase().contains((String)me.getKey()) && !character_list.contains(show_for_character[0]))
							{
								character_list+=show_for_character[0]+",";
								if(!show_list.contains(show_for_character[1]))
								{
									show_list+=show_for_character[1]+",";
								}
							}
						}
						obj_tweet.put("keywords", keywords);
						obj_tweet.put("show_list", show_list);
						obj_tweet.put("character_list", character_list);
						String stringdate=obj_tweet.getString("created_at");
						obj_tweet.remove("created_at");
						stringdate=stringdate.replace("+0000","IST");
						Date d=sdf.parse(stringdate);
						String created_at=dateFormat.format(d);
						obj_tweet.put("created_at",created_at);
						rawJSON=obj_tweet.toString();
						storeJSON(rawJSON+"\n\n","show.txt");
						TweetQueue.tweetQueue.add(rawJSON);
						}//End of for(Status tweet:tweets)
					
						if(tweets.size()>1)	
						since=String.valueOf(tweets.get(tweets.size()-1).getId());
						if(!isChar)
						{
							int item_no=show.getElementsByTagName("since").getLength();
							show.getElementsByTagName("since").item(item_no-1).setTextContent(since);
						}
						else
						character.getElementsByTagName("since").item(0).setTextContent(since);
						TransformerFactory transformerFactory = TransformerFactory.newInstance();
						Transformer transformer = transformerFactory.newTransformer();
						DOMSource domSource = new DOMSource(doc);
						StreamResult streamResult = new StreamResult(fXmlFile);
						transformer.transform(domSource, streamResult);
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
				}while((query = result.nextQuery())!=null);
//				if(tweets.size()>1)
//				since=String.valueOf(tweets.get(tweets.size()-1).getId());
//				if(!isChar)
//				{
//					int item_no=show.getElementsByTagName("since").getLength();
//					show.getElementsByTagName("since").item(item_no-1).setTextContent(since);
//				}
//				else
//				character.getElementsByTagName("since").item(0).setTextContent(since);
//				TransformerFactory transformerFactory = TransformerFactory.newInstance();
//				Transformer transformer = transformerFactory.newTransformer();
//				DOMSource domSource = new DOMSource(doc);
//				StreamResult streamResult = new StreamResult(fXmlFile);
//				transformer.transform(domSource, streamResult);
			}//End of for loop
		}	//End of while true loop
	}//End of main function
}//End of class
