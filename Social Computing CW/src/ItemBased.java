import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


/**
 * 
 * @author Rafael and Alexis
 * Item-based collaborative filtering recommender system
 */
public class ItemBased {
	
	//The following 2 hashmaps stores the users who have both rated item1 and item2, alongside the actual ratings
	//We make sure the keys are always the same
	public HashMap<Integer,Integer> item1Ratings;
	public HashMap<Integer,Integer> item2Ratings;
	
	public Connection c;

	
	public ItemBased(Connection con) {
		c = con;
		item1Ratings = new HashMap<Integer,Integer>();
		item2Ratings = new HashMap<Integer,Integer>();
		
	}
	
	/**
	 * If the similarity between item1 and item2 has already been calculated, retrieve it from the database. Otherwise, calculate its value and save it in the database before returning it
	 * @param item1
	 * @param item2
	 * @param allRatings The entire data structure containing all the ratings
	 * @param avgRatings The average user ratings
	 * @return The similarity between item1 and item2
	 */
	public float getSimilarity(int item1, int item2, HashMap<Integer, HashMap<Integer,Integer>> allRatings, HashMap<Integer,Float> avgRatings) {
		
		//similarity between an item and itself is always 1
		if (item1 == item2) return 1.0f;
		
		//sort the index to avoid duplicate values in the database ( similarity(i,j) == similarity(j,i) )
		int smallestIndex = item1 < item2 ? item1 : item2;
		int greaterIndex = item1 < item2 ? item2 : item1;
		
		float similarity = 0.0f;
		boolean found = false;
		
		
		//try to retrieve similarity from database
		String query = "SELECT * FROM similarities WHERE item1 = ? AND item2 = ?";
		try {
			PreparedStatement ps1 = c.prepareStatement(query);
			ps1.setInt(1, smallestIndex);
			ps1.setInt(2, greaterIndex);
			ResultSet rs = ps1.executeQuery();
			while (rs.next()) {
				similarity = rs.getFloat(3);
				found = true;
			}
			
			if (found) {
				//if we found the similarity in the database, we can return it
				ps1.close();
				rs.close();
				return similarity;
			}
		} 
		
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
		//if we didn't return anything, we didn't find the similarity
		
		//get users who have both rated item1 and item2
		getRelevantUsersWithRatings(smallestIndex, greaterIndex, allRatings);
		
		float numerator = 0;
		double denominator1 = 0;
		double denominator2 = 0;
		
		for (Integer user : item1Ratings.keySet()) { //equal to the keys of the second hash map
			int item1Rating = item1Ratings.get(user);
			int item2Rating = item2Ratings.get(user);
			
			float avgRating = avgRatings.get(user);
			
			numerator += (item1Rating - avgRating) * (item2Rating - avgRating);
			denominator1 += Math.pow(item1Rating - avgRating, 2);
			denominator2 += Math.pow(item2Rating - avgRating, 2);
		}
		
		denominator1 =  Math.sqrt(denominator1);
		denominator2 = Math.sqrt(denominator2);
		similarity = 0.0f;
		if (denominator1 * denominator2 != 0) { //if (denominator != 0)
			similarity = (float) (numerator / (denominator1 * denominator2)) ;
		}
		//i.e., if denominator == 0, then similarity = 0 as previously defined
			
		//now that we calculated the similarity, save it in the database before returning
		String insertQuery = "INSERT INTO similarities (item1, item2, similarity) VALUES (?, ?, ?)";
		try {
			PreparedStatement ps2 = c.prepareStatement(insertQuery);
			ps2.setInt(1, smallestIndex);
			ps2.setInt(2, greaterIndex);
			ps2.setFloat(3, similarity);
			ps2.executeUpdate();
			ps2.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return similarity;
	}
	
	/**
	 * Returns the predicted rating for a given item by a given user and saves it in the database (table "predictions"). This method does not consider negative similarities during the calculations
	 * @param user
	 * @param item
	 * @param allRatings The entire data structure containing all the ratings
	 * @param avgRatings The average user ratings
	 * @return The predicted rating for 'item' by 'user'
	 */
	public float getPrediction(int user, int item, HashMap<Integer, HashMap<Integer,Integer>> allRatings, HashMap<Integer,Float> avgRatings) {
		
		float numerator = 0;
		float denominator = 0;
	
		for (Integer it : allRatings.keySet()) { //for every item
			HashMap<Integer,Integer> usersAndRatingsForIt = allRatings.get(it); //every user who have rated "it" and the actual rating
			
			if (usersAndRatingsForIt.containsKey(user)) { //if user have rated the item
				float similarity = getSimilarity(it, item, allRatings, avgRatings);
				
				//we exclude negative similarities in this method
				if (similarity >= 0) {
					int rating = usersAndRatingsForIt.get(user);
					numerator += similarity * rating;
					denominator += similarity;
				}
			}
		}
	
		float prediction = Float.NaN;
		
		prediction = numerator / denominator;
		
		//prediction might still be NaN at this point
		
		if (Float.isNaN(prediction) && avgRatings.containsKey(user)) {
			prediction = avgRatings.get(user);
		}
		
		else if (Float.isNaN(prediction) && !avgRatings.containsKey(user)) {
			//assign item average rating as the predicted rating
			float num = 0;
			int n = 0;
			HashMap<Integer, Integer> usersAndRatings = allRatings.get(item);
			for (Integer u : usersAndRatings.keySet()) {
				int r = usersAndRatings.get(u);
				num += r;
				n++;
			}
			
			prediction = num/n;
			
		}
		
		//rating ranges from 1 to 5
		else if (prediction > 5) prediction = 5;
		else if (prediction < 1) prediction = 1;
		
		//save it in the database before returning
		String updateQuery = "UPDATE predictions SET prediction = ? WHERE userid = ? AND profileid = ?";
		try {
			PreparedStatement ps = c.prepareStatement(updateQuery);
			ps.setFloat(1, prediction);
			ps.setInt(2, user);
			ps.setInt(3, item);
			ps.executeUpdate();
//			c.commit();
			ps.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return prediction;
	}
	
	/**
	 * Returns the predicted rating for a given item by a given user and saves it in the database (table "predictions2"). This method considers negative similarities during the calculations
	 * @param user
	 * @param item
	 * @param allRatings The entire data structure containing all the ratings
	 * @param avgRatings The average user ratings
	 * @return The predicted rating for 'item' by 'user'
	 */
	public float getPredictionConsideringNegativeSims(int user, int item, HashMap<Integer, HashMap<Integer,Integer>> allRatings, HashMap<Integer,Float> avgRatings) {
		
		float prediction = Float.NaN;
		
		//if the user does not have an average rating, it means he didn't rate any item. In this case, we assign the item average rating as the predicted rating
		if (!avgRatings.containsKey(user)) {
			float num = 0;
			int n = 0;
			HashMap<Integer, Integer> usersAndRatings = allRatings.get(item);
			for (Integer u : usersAndRatings.keySet()) {
				int r = usersAndRatings.get(u);
				num += r;
				n++;
			}
			
			prediction = num/n;
		}
		
		else {
		
			float numerator = 0;
			float denominator = 0;
			
			//in order to consider the negative similarities without decreasing the overall accuracy we use the baseline predictor
			float baselinePredictor = getBaselinePredictor(user, item, allRatings, avgRatings);
		
			for (Integer it : allRatings.keySet()) { //for every item
				HashMap<Integer,Integer> usersAndRatingsForIt = allRatings.get(it); //every user who have rated "it" and the actual rating
				
				if (usersAndRatingsForIt.containsKey(user)) { //if user have rated the item
					float similarity = getSimilarity(it, item, allRatings, avgRatings);
					int rating = usersAndRatingsForIt.get(user);
					
					
					numerator += similarity * (rating - baselinePredictor);
					denominator += Math.abs(similarity);
					
				}
			}
		
			
			
			prediction = (numerator / denominator) + baselinePredictor;
			
			//prediction might still be NaN at this point
			if (Float.isNaN(prediction)) {
				prediction = avgRatings.get(user);
				//note that we already made sure the user has an average rating
			}
			
			//rating ranges from 1 to 5
			else if (prediction > 5) prediction = 5;
			else if (prediction < 1) prediction = 1;
		}
		
		//save it in the database before returning
		String updateQuery = "UPDATE predictions2 SET prediction = ? WHERE userid = ? AND profileid = ?";
		try {
			PreparedStatement ps = c.prepareStatement(updateQuery);
			ps.setFloat(1, prediction);
			ps.setInt(2, user);
			ps.setInt(3, item);
			ps.executeUpdate();
//			c.commit();
			ps.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return prediction;
	}
	
	/**
	 * Calculates the mean absolute error for a set of predictions for which we know the actual true value. Note that all the entries in the "predictions" hash map must also be in the "allRatings" hash map (obviously, the rating - and only the rating - may differ)
	 * @param predictions The set of predictions
	 * @param allRatings The entire data structure containing all the ratings
	 * @return The mean absolute error (MAE)
	 */
	public float getMeanAbsoluteError(HashMap<Integer, HashMap<Integer,Float>> predictions, HashMap<Integer, HashMap<Integer,Integer>> allRatings) {
		
		int n = 0;
		float numerator = 0;
		
		for (Integer item : predictions.keySet()) {
			HashMap<Integer,Float> usersAndPreds = predictions.get(item);
			for (Integer user : usersAndPreds.keySet()) {
				float pred = usersAndPreds.get(user); //the predicted rating for 'item' by 'user'
				
				int realRating = allRatings.get(item).get(user);
				
				numerator += Math.abs(pred - realRating);
				n++;
			}
		}
		
		float mae = numerator / n;
		return mae;
	}
	
	/**
	 * Finds all the users who have rated both item1 and item2 and save their ratings for these 2 items in "item1Ratings" and "item2Ratings" hash maps (class attributes) 
	 * @param item1
	 * @param item2
	 * @param allRatings The entire data structure containing all the ratings
	 */
	private void getRelevantUsersWithRatings(int item1, int item2, HashMap<Integer, HashMap<Integer,Integer>> allRatings) {
		
		item1Ratings.clear();
		item2Ratings.clear();
		
		HashMap<Integer,Integer> i1Ratings = allRatings.get(item1);
		HashMap<Integer,Integer> i2Ratings = allRatings.get(item2);
		for (Integer user : i1Ratings.keySet()) {
			if (i2Ratings.containsKey(user)) {
				item1Ratings.put(user, i1Ratings.get(user));
				item2Ratings.put(user, i2Ratings.get(user));
			}
		}
	}
	
	/**
	 * Calculates the baseline predictor to be used in the process of predicting ratings considering negative similarities
	 * @param user
	 * @param item
	 * @param allRatings The entire data structure containing all the ratings
	 * @param avgRatings The average user ratings
	 * @return The baseline predictor for this specific pair of item and user
	 */
	private float getBaselinePredictor(int user, int item, HashMap<Integer, HashMap<Integer,Integer>> allRatings, HashMap<Integer,Float> avgRatings) {
		int numItemsRated = 0;
		float userBaselinePredictor = 0;
		
		for (Integer it : allRatings.keySet()) { //for every item
			HashMap<Integer,Integer> usersAndRatingsForIt = allRatings.get(it); //every user who have rated "it" and the actual rating
			
			if (usersAndRatingsForIt.containsKey(user)) { //if user have rated the item
				numItemsRated++;
				int rating = usersAndRatingsForIt.get(user);
				userBaselinePredictor += (rating - avgRatings.get(user));
			}
		}
		
		userBaselinePredictor = userBaselinePredictor / numItemsRated;
		
		float itemBaselinePredictor = 0;
		
		HashMap<Integer,Integer> usersThatRatedIt = allRatings.get(item); //every user who have rated "item" and the actual rating
		int numUsersRated = usersThatRatedIt.size();
		for (Integer u : usersThatRatedIt.keySet()) {
			int rating = usersThatRatedIt.get(u);
			itemBaselinePredictor += rating - userBaselinePredictor - avgRatings.get(user);
		}
		
		itemBaselinePredictor = itemBaselinePredictor / numUsersRated;	
		
		float baselinePredictor = itemBaselinePredictor + userBaselinePredictor + avgRatings.get(user);
		return baselinePredictor;
	}
}

