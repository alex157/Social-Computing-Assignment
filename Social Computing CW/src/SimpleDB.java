import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SimpleDB {
	
	//If you wish  to run the code, provide the proper connection string
	final String connection_string = "jdbc:sqlite:C:/Users/Rafael/socialcomputing/cw.db";
//	final String connection_string = "jdbc:sqlite:/home/rgr1u13/socialcomputing/cw.db";
//	final String connection_string = "jdbc:sqlite:/home/aa25g12/SC coursework/Database/cw.db";

	
	public Connection c;
	
	public SimpleDB(){
		
		try{
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(connection_string);
			c.setAutoCommit(false);
			Statement stat = c.createStatement();
			stat.execute("PRAGMA journal_mode=off");
			stat.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Returns all the ratings from the training set from the database in a HashMap<Integer<HashMap<Integer,Integer>>
	 * Outer hash map: key = item; value = hash map containing users who rated the item and the actual ratings
	 * Inner hash map: key = user; value = rating for that item by this user
	 * @return All the ratings from the training set
	 */
	public HashMap<Integer, HashMap<Integer,Integer>> loadRatings(){
		HashMap<Integer, HashMap<Integer,Integer>> ratings = new HashMap<Integer, HashMap<Integer,Integer>>();
		
		try{
			System.out.println("Loading ratings...");
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("select * from alldata");
			System.out.println("executed query");
			
			
			while(rs.next()){

				int item = rs.getInt(2);
				int user = rs.getInt(1);
				int rating = rs.getInt(3);
				HashMap<Integer,Integer> usersAndRatings;
				if (ratings.containsKey(item)) {
					usersAndRatings = ratings.get(item);
					usersAndRatings.put(user, rating);
				} else {
					usersAndRatings = new HashMap<Integer, Integer>();
					usersAndRatings.put(user, rating);
					ratings.put(item, usersAndRatings);
				}
				
			}
			System.out.println("finished caching");
			rs.close();
			s.close();
				
			}catch(Exception e){
				
				e.printStackTrace();
			}
		
		return ratings;
	}
	
	/**
	 * Method only used for evaluation
	 * @return
	 */
	public HashMap<Integer, HashMap<Integer,Float>> loadPredictions() {
		HashMap<Integer, HashMap<Integer,Float>> preds = new HashMap<Integer, HashMap<Integer,Float>>();
		
		try{
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("select * from testdata2");
			
			while(rs.next()){

				int item = rs.getInt(2);
				int user = rs.getInt(1);
				float pred = rs.getFloat(3);
				HashMap<Integer,Float> usersAndRatings;
				if (preds.containsKey(item)) {
					usersAndRatings = preds.get(item);
					usersAndRatings.put(user, pred);
				} else {
					usersAndRatings = new HashMap<Integer, Float>();
					usersAndRatings.put(user, pred);
					preds.put(item, usersAndRatings);
				}
				
			}
			rs.close();
			s.close();
				
			}catch(Exception e){
				
				e.printStackTrace();
			}
		
		return preds;
	}
	
	/**
	 * Retrieve from the database (table "predictions") the predictions that are left to be made. The inner static array of ints is structured like the following:
	 * index 0: user id
	 * index 1: item id
	 * @return The missing predictionms
	 */
	public ArrayList<int[]> getPredictionsToMake(){
		ArrayList<int[]> preds = new ArrayList<>();
		
		try{
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("select * from predictions"); 
			while(rs.next()){
				if (rs.getFloat(3) == 0) {
					int[] pred = new int[2];
					pred[0] = rs.getInt(1);
					pred[1] = rs.getInt(2);
					preds.add(pred);
				}
			}
			rs.close();
			s.close();
			}catch(Exception e){
				
				e.printStackTrace();
			}
		
		return preds;
	}
	
	/**
	 * Retrieve from the database (table "predictions2") the predictions that are left to be made. The inner static array of ints is structured like the following:
	 * index 0: user id
	 * index 1: item id
	 * @return The missing predictions
	 */
	public ArrayList<int[]> getPredictionsToMake2(){
		ArrayList<int[]> preds = new ArrayList<>();
		
		try{
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("select * from predictions2"); 
			while(rs.next()){
				if (rs.getFloat(3) == 0 ) {
					int[] pred = new int[2];
					pred[0] = rs.getInt(1);
					pred[1] = rs.getInt(2);
					preds.add(pred);
				}
			}
			rs.close();
			s.close();
			}catch(Exception e){
				
				e.printStackTrace();
			}
		
		return preds;
	}
	
	/**
	 * 
	 * @return The average user ratings
	 * key = user; value = average rating
	 */
	public HashMap<Integer,Float> getAvgUserRatings() {
		HashMap<Integer,Float> avgRatings = new HashMap<Integer,Float>();

		try {
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("select * from avguserratings");
			
			while (rs.next()) {
				int user = rs.getInt("userid");
				float rating = rs.getFloat("rating");
				avgRatings.put(user,rating);
			}
			rs.close();
			s.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		return avgRatings;
	}
	
	/**
	 * Retrieves the similarities from the from the "similarities" table in the database in a HashMap<HashMap<Integer,Integer>, Float>
	 * Outer hash map: key = hash map that is only a pair of items; value = similarities
	 * @return The similarities 
	 */
	public HashMap<HashMap<Integer,Integer>, Float> getSimilarities() {
		
		HashMap<HashMap<Integer,Integer>, Float> similarities = new HashMap<HashMap<Integer,Integer>, Float>();
		
		try {
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("select * from similarities");
			
			
			while (rs.next()) {
				int item1 = rs.getInt("item1");
				int item2 = rs.getInt("item2");
				float similarity = rs.getFloat("similarity");
				HashMap<Integer,Integer> item1AndItem2 = new HashMap<Integer, Integer>();
				item1AndItem2.put(item1, item2);
				similarities.put(item1AndItem2,similarity);
			}
			rs.close();
			s.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return similarities;
	}
	
	/**
	 * The main method was used during implementation, testing and evaluation processes. There is probably nothing relevant here for the matter of assessment.
	 * Because of  this, we have commented everything out
	 */
	public static void main(String[] args){
		/* ==================================================================================================================================
		SimpleDB s1 = new SimpleDB();
		HashMap<Integer, HashMap<Integer,Integer>> allRatings = s1.loadRatings();
		System.out.println("Total number of items = " + Integer.toString(allRatings.size()));
		HashMap<Integer,Float> avgRatings = s1.getAvgUserRatings();
		System.out.println("Total number of users (number of average ratings) = " + Integer.toString(avgRatings.size()));
		
		ArrayList <int[]> preds = s1.getPredictionsToMake();
		System.out.println("Number of predictions to be found = " + Integer.toString(preds.size()));
		
		ArrayList <int[]> preds2 = s1.getPredictionsToMake2();
		System.out.println("Number of predictions 2 to be found = " + Integer.toString(preds2.size()));
		
		System.out.println("============================");
		
		ItemBased ib = new ItemBased(s1.c);

		HashMap<Integer, HashMap<Integer,Float>> predictionsMade = s1.loadPredictions();
		float mae = ib.getMeanAbsoluteError(predictionsMade, allRatings);
		System.out.println("MAE = " + Float.toString(mae));
		
		System.out.println();
		
		try {
			
			int times = 0;
			for (int i=0; i<preds2.size(); i++) {
//				float pred = ib.getPrediction(preds.get(i)[0], preds.get(i)[1], allRatings, avgRatings);
				float pred2 = ib.getPredictionConsideringNegativeSims(preds2.get(i)[0], preds2.get(i)[1], allRatings, avgRatings);
				times++;
				System.out.println("user " + Integer.toString(preds2.get(i)[0])+ " has a predicted rating of " + Float.toString(pred2) + " for item " + Integer.toString(preds2.get(i)[1]) );
				if (times == 20) {
					ib.c.commit();
					times = 0;
				}
			}
			System.out.println("PROGRAM FINISHED!!!");
			s1.c.commit();
			
			
			
			s1.c.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		try {
//			final int numberOfItems = 30000;
//			boolean[] calculatedBefore = new boolean[numberOfItems + 1];
//			
//			for (Integer item1 : allRatings.keySet()) {
//				System.out.println(item1);
//				System.out.println("===========================================================================================");
//				
//				for (Integer item2 : allRatings.keySet()) {
//					if (!calculatedBefore[item2]) {
//						float sim = ib.getSimilarity(item1, item2, allRatings, avgRatings);
//					}
//				}
////				System.out.println("commiting...");
//				calculatedBefore[item1] = true;
//				ib.c.commit();
//			}
//			System.out.println("FINISHED PROGRAM!!!");
//			s1.c.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	================================================================================================================================== */	
	}
}