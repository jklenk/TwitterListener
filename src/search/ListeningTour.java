package search;
// main thread
// author @klenk

import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;

import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.conf.ConfigurationBuilder;
//import twitter4j.GeoLocation;

public class ListeningTour {

	// instantiate config builder and the data layer
	private ConfigurationBuilder cb;
	DBConnect conn = new DBConnect();
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		
		// invoke self from main thread
		ListeningTour taskObj = new ListeningTour();
		
		taskObj.loadMenu();
	}
	

	public void loadMenu() throws SQLException, InterruptedException{

		this.readTwitterFeed();
		
	}

	// Used for Core. Batch capture by query. Geofencing available.
	public void getTweetByQuery(String keyword) throws InterruptedException, SQLException {
		
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		
		//double latitude  = 44.058173;
		//double longitude = -121.315310;

		if(cb != null) {
			
			try {
				
				Query query = new Query(keyword);
				query.setCount(100);
				//query.setGeoCode(new GeoLocation(latitude, longitude), 200, Query.MILES);
				//query.until(conn.getLastDate());
				query.since("2015-08-30");
				QueryResult result;
				result = twitter.search(query);
				System.out.println("Getting tweets...");
				
				List<Status> tweets = result.getTweets();
				
				//insert new row per tweet
				
				for(Status tweet : tweets) {
					
					if(tweet.getGeoLocation() != null){
						System.out.println("GeoLocation Attached.");
						this.conn.insertNewWithGeoLocation(tweet.getText(),
								        tweet.getUser().getName().toString(),
								        tweet.getUser().getLocation(),
										String.valueOf(tweet.getGeoLocation().getLatitude()),
										String.valueOf(tweet.getGeoLocation().getLongitude()),
							            tweet.getLang(), 
							            tweet.isFavorited(),
							            tweet.getFavoriteCount(),
							            tweet.getUser().getFollowersCount(),
							            tweet.getUser().getTimeZone(),
							            tweet.getUser().isVerified(),
							            tweet.getCreatedAt());
					}else{
						
						this.conn.insertNew(tweet.getText(), 
								            tweet.getUser().getName().toString(),
								            tweet.getUser().getLocation(),
								            tweet.getLang(),
								            tweet.isFavorited(),
								            tweet.getFavoriteCount(),
								            tweet.getUser().getFollowersCount(),
								            tweet.getUser().getTimeZone(),
								            tweet.getUser().isVerified(),
								            tweet.getCreatedAt());
						
						System.out.println("No GeoLocation Data Found.");
					}
					
				}
			}catch (TwitterException te){
				
				System.out.println("TEError Code: " + te.getErrorCode());
				System.out.println("TEException Code: " + te.getExceptionCode());
				System.out.println("TEStatus Code: " + te.getStatusCode());
				if(te.getStatusCode() == 401){
					System.out.println("Twitter Error: \nAutentication credentials (https://dev.twitter.com/pages/auth) were missing or incorrect.\nEnsure that you have set valid consumer key/secret, access token/secret, and the system clock is in sync.");
				}else{
					System.out.println("Twitter Error: " + te.getMessage());
				}
			}
		}
		
		
		
	}
	

	// Used by Streams
	public void readTwitterFeed() {
		
		TwitterStream stream = TwitterConnectionBuilderUtil.getStream();
		
		StatusListener listener = new StatusListener(){

			@Override
			public void onException(Exception e) {
				System.out.println("Exception occurred: " + e.getMessage());
				e.printStackTrace();
				
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				System.out.println("Status Deletion Notice.");
				
			}
			
			// Used if geoLocation data removed forcibly by Twitter
			@Override
			public void onScrubGeo(long arg0, long arg1) {
				System.out.println("Scrub Geo With: " + arg0 +":" + arg1);
				
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				System.out.println("Stall Warning...");
				
			}

			@Override
			public void onStatus(Status status) {
				
				System.out.println("Tweet.");
				if(status.getGeoLocation() != null){
					System.out.println("GeoLocation Attached.");
					try {
						
			
						conn.insertNewWithGeoLocation(status.getText(),
								        status.getUser().getName().toString(),
								        status.getUser().getLocation(),
										String.valueOf(status.getGeoLocation().getLatitude()),
										String.valueOf(status.getGeoLocation().getLongitude()),
							            status.getLang(), 
							            status.isFavorited(),
							            status.getFavoriteCount(),
							            status.getUser().getFollowersCount(),
							            status.getUser().getTimeZone().toString(),
							            status.getUser().isVerified(),
							            status.getCreatedAt());
					} catch (SQLException e) {
						System.out.println("SQL Exception: " + e.getMessage());
						e.printStackTrace();
					}
				}else{						
					try {
						
						conn.insertNew(status.getText(), 
								status.getUser().getName().toString(),
								status.getUser().getLocation(),
						        status.getLang(),
						        status.isFavorited(),
						        status.getFavoriteCount(),
						        status.getUser().getFollowersCount(),
						        status.getUser().getTimeZone(),
						        status.getUser().isVerified(),
						        status.getCreatedAt());
					} catch (SQLException e) {
						System.out.println("SQL Exception: " + e.getMessage());
						e.printStackTrace();
					}
					System.out.println("No GeoLocation Data Found.");
					
				}
				
				
				
			}

			@Override
			public void onTrackLimitationNotice(int arg0) {
				System.out.println("Track Limitation Notice For: " + arg0);
				
			}
			
			
		};
		
		/*Possible query terms:
		 * #getmetocollege
		 * #uofreshmanprobs
		 * #eugene
		 * #uo
		 * #universityoforegon
		 * #introDucktion
		 * #college
		*/
		
		FilterQuery query = new FilterQuery();
		
	
		String[] uOfOKeywords = {"#roadtouo",
								 "#goducks",
								 "#oregon",
								"#futureoregonduck",
								"#oregonducks",
								"#beanoregonduck",
								"@beanoregonduck",
								"#ilovemyducks",
								"#uoregon",
								"#wintheday",
								 "#ducks",
								"#oregonbound",
								"#Callmeaduck",
								"@callmeaduck",
								"#hellouo",
								"#idoregon",
								"#exploregon",
								"#eug",
								"#eugene",
								"#duckweather",
								"#wtd",
								"#mightyoregon",
								"#niceweatherforducks",
								"#itseasybeinggreen",
								"#itseasybeingineugene",
								"#ducksunlimited",
								"#duckcountry",
								"#walklikeaduck",
								"#aroundtheo",
								"#talklikeaduck",
								"#quackattack",
								"#quackedup",
								"#fastasduck",
								"#theduckzone",
								"#greennyellow",
								"#greenandyellow",
								"#yello",
								"#duckoff",
								"#uo",
								"@Univ_of_Oregon",
								"University Of Oregon",
								"uofo"
								};

		query.track(uOfOKeywords);
		
		stream.addListener(listener);
		
		// necessary for connection. only Official Twitter Partners get full firehose access.
		stream.filter(query);
	}
	
}


