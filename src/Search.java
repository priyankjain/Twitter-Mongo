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

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.json.DataObjectFactory;

public final class Search {

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
			FileNotFoundException, IOException, SAXException,
			ParserConfigurationException {
		File fXmlFile = new File("xml/channels.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		NodeList nList = doc.getElementsByTagName("channel");
		Twitter twitter = new TwitterFactory().getSingleton();
		Properties prop = new Properties();
		prop.load(new FileInputStream("twitter4j.properties"));
		for (int temp = 0; temp < nList.getLength(); temp++) {
			Node nNode = nList.item(temp);
			Element e = (Element) nNode;
			NodeList showList = e.getElementsByTagName("show");
			for (int shownumber = 0; shownumber < showList.getLength(); shownumber++) {
				Element show = (Element) showList.item(shownumber);
				String followList[] = show.getElementsByTagName("follow")
						.item(0).getTextContent().split(",");
				String keywordsList[] = show.getElementsByTagName("keywords")
						.item(0).getTextContent().split(",");
				long idList[] = new long[followList.length];
				for (int i = 0; i < followList.length; i++) {
					idList[i] = twitter.showUser(followList[i]).getId();
				}
				String hashtagList[] = show.getElementsByTagName("hashtags")
						.item(0).getTextContent().split(",");
				String mentionList[] = show.getElementsByTagName("mentions")
						.item(0).getTextContent().split(",");
				String start = show.getElementsByTagName("start").item(0)
						.getTextContent();
				for (int i = 0; i < keywordsList.length; i+=10) {
					String q = "";
					for (int j = i; j < keywordsList.length; j++) {
						q += followList[(i + j)] + " OR ";
					}
					q = q.substring(0, q.length()-4);
					System.out.println("i = "+i);
					Query query = new Query(q);
					query.since(start);
					QueryResult result;
					do {
						result = twitter.search(query);
						List<Status> tweets = result.getTweets();
						for (Status tweet : tweets) {
							String rawJSON = DataObjectFactory.getRawJSON(tweet);
							System.out.println("This is raw JSON: "+rawJSON);
							storeJSON(rawJSON,"BiggBoss.json");
						}
					} while ((query = result.nextQuery()) != null);
				}
			}
		}
	}
}
