package search;

// twitter config builder
// author @klenk

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterConnectionBuilderUtil {
	
	public static TwitterStream getStream(){
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey(<OAuthConsumerKey>);
		cb.setOAuthConsumerSecret(<OAuthConsumerSecret>);
		cb.setOAuthAccessToken(<OAuthAccessToken>);
		cb.setOAuthAccessTokenSecret(<OAuthAccessTokenSecret>);
		
		return new TwitterStreamFactory(cb.build()).getInstance();
		

	}
	
}
