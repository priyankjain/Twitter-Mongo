/*
 * Copyright 2007 Yusuke Yamamoto
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public final class Streaming {
	
	public static void main(String[] args) throws TwitterException, InterruptedException,
			FileNotFoundException, IOException, SAXException,
			ParserConfigurationException {
		File fXmlFile = new File("xml/channels.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		NodeList nList = doc.getElementsByTagName("channel");
		Twitter twitter=new TwitterFactory().getSingleton();
		Properties prop = new Properties();
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				System.out.println(status.toString());
			}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:"
						+ statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:"
						+ numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId
						+ " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				System.out.println("Got stall warning:" + arg0);
			}
		};
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(listener);
		prop.load(new FileInputStream("twitter4j.properties"));
		for (int temp = 0; temp < nList.getLength(); temp++) {
			Node nNode = nList.item(temp);
			Element e=(Element)nNode;
			NodeList showList=e.getElementsByTagName("show");
			int count=1000;
			Vector<String> keywordList=new Vector(100);
			for(int shownumber=0;shownumber<showList.getLength() && count>0;shownumber++)
			{
				Element show=(Element)showList.item(shownumber);
				String[] trackList=show.getElementsByTagName("keywords").item(0).getTextContent().split(",");
				for(String handle: trackList)
				{
					keywordList.add(handle);
				}
				//String keywordsList[]=show.getElementsByTagName("keywords").item(0).getTextContent().split(",");
//				long idList[]=new long[followList.length];
//				for(int i=0;i<followList.length;i++)
//				{
//					idList[i]=twitter.showUser(followList[i]).getId();
//				}
				//twitterStream.filter(new FilterQuery().track(keywordsList));

			}
			String[] a=new String[keywordList.size()];
			twitterStream.filter(new FilterQuery().track(keywordList.toArray(a)));
		}	
	}
}
