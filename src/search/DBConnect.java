package search;
// data layer
// author @klenk

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DBConnect {
	
	Connection conn = null;
	
	String dbTable = <dbTable>;

	
	public void openDB(){
		
		try {
			String dbURL = <connectionString>;
			String user  = <user>;
			String pass  = <pass>;
			conn = DriverManager.getConnection(dbURL, user, pass);
		}catch (SQLException ex){
			ex.printStackTrace();
		}
		
	}
	
	// database connection closer
	public void closeDB(){
		
		try {
			
			if(conn != null){
				
				conn.close();
			}
		}catch (SQLException ex){
			ex.printStackTrace();
		}
	}
	
	// insert function - GeoLocation must be attached (Lat/Long)
	public void insertNewWithGeoLocation(String tweetText, String tweetUser, String tweetUserLoc, String latitude, String longitude, String language, boolean favorite, 
			int favCount, int followCount, String timeZone, boolean verified, Date dateTime) throws SQLException{
		
		// date formatting
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.sss");
		
		// open database connection
		this.openDB();
		
		// sql statement creation
		Statement st = conn.createStatement();
		try {
			
			String properText = null;
			
			// remove apostrophes for INSERT
			properText = tweetText.replaceAll("'", "");
			
		    st.executeUpdate("INSERT INTO " + dbTable + " VALUES ('" +
				       properText + "', '" +
				       tweetUser + "', '" +
				    tweetUserLoc + "', '" +
				        latitude + "', '" +
				       longitude + "', '" +
				        language + "', '" +
						favorite + "', " +
						favCount + ", " +
						followCount + ", '" +
						timeZone + "', '" +
						verified + "', '" +
				        dateFormat.format(dateTime) + "')");
		} catch (SQLException ex){		
			ex.printStackTrace();		
		}finally {
			this.closeDB();
		}
		
	}
	
	// insert function, if no Lat/Long exists
	public void insertNew(String tweetText, String tweetUser, String tweetUserLoc, String language, boolean favorite, int favCount, int followCount, 
			String timeZone, boolean verified, Date dateTime) throws SQLException{
		

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.sss");
		
		// open database connection
		this.openDB();
		
		// sql statement creation
		Statement st = conn.createStatement();
		try {
			
			String properText = null;
			
			properText = tweetText.replaceAll("'", "");
			
		    st.executeUpdate("INSERT INTO " + dbTable + " VALUES ('" +
				       properText + "', '" +
				       tweetUser + "', '" +   
				    tweetUserLoc + "', null, null, '" +
				        language + "', '" +
						favorite + "', '" +
						favCount + "', '" +
						followCount + "', '" +
						timeZone + "', '" +
						verified + "', '" +
				        dateFormat.format(dateTime) + "')");
		} catch (SQLException ex){	
			
			// debug statements, if the insert fails, i want to know why
			System.out.println("SQL Exception: " + ex.getMessage());
			System.out.println("Insert Statement: INSERT INTO " + dbTable + " values ('" + tweetText + "', '" + tweetUser + "', null, null, '" + language + "', '" +
								favorite + "', '" + favCount + "', '" + followCount + "', '" + timeZone + "', '" + verified + "', '" + dateFormat.format(dateTime) + "')");
		}finally {
			// close database connection regardless of success
			this.closeDB();
		}
		
	}
	
	// function to return the most recent date. used for Core, batch process uses this to get a new 'From' date.
	public String getLastDate() {
		

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat inputFormat = new SimpleDateFormat("yyy-MM-dd hh:mm:ss.sss");
		
		Date date = new Date();
		String lastDate = null;
		
		this.openDB();
		
		String query = "SELECT TweetSent FROM " + dbTable + " WHERE TweetID = (SELECT MAX(TweetID) FROM " + dbTable + ")";
		try {
			
			Statement st = conn.createStatement();
			ResultSet result = st.executeQuery(query);
			
			while(result.next()){
				
				Date resultDate = inputFormat.parse(result.getString("TweetSent"));
				lastDate = dateFormat.format(resultDate);
				System.out.println(result.getString("TweetSent"));
			}
			
		}catch (SQLException ex){
		
			System.out.println("Message: " + ex.getMessage());
			System.out.println("Setting date to current system date/time...");

			lastDate = dateFormat.format(date);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			this.closeDB();
		}
		
		
		return lastDate;
		
	}

}
