package rocksdb;


import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;

import org.rocksdb.RocksIterator;


public class tweet implements Runnable {
	
	static String[]  users = {"voxdotcom", "breakingnews", "cnnbrk", "Reuters" };
	int start = 0;
	int end  = 0;
	tweetstore tw = tweetstore.getInstance();

	public tweet(int s, int e) {
		// TODO Auto-generated constructor stub
		start = s;
		end = e;
	}

	public static void main(String[] args) {
		tweet tw = new tweet(0,4);
		//tw.run();
		tw.display("voxdotcom");
	}

	public void display(String user) {
		tw.display(user);
	}
	
	public void delete() {
		tw.delete();
	}
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("")
		  .setOAuthConsumerSecret("")
		  .setOAuthAccessToken("")
		  .setOAuthAccessTokenSecret("");
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		
		
        try {
            List<Status> statuses;
            while (start < end) {
	            //user ="realDonaldTrump";
            	String user = users[start++];
            	for(int k = 1; k < 5; k++) {
	            Paging pg  = new Paging(k, 100);
	            statuses = twitter.getUserTimeline(user, pg);
	            
	            for (Status status : statuses) {
	                //System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
	            	tw.insert(status.getUser().getScreenName(), status.getText());
	             }
            	}
            }
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get timeline: " + te.getMessage());
            System.exit(-1);
        }
	}
	
	

}
