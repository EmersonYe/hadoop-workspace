package youtube;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.TreeMap;

public class MRreducer  extends Reducer <Text,Text,Text,Text> {
	public static String IFS=",";
	public static String OFS=",";
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		// TODO 1: initialize variables
		int temp_category_id, temp_views, temp_likes, temp_dislikes;
		String compositeString;
		String[] compositeStringArray;
		TreeMap<Integer,String[]> most_views= new TreeMap<Integer,String[]>();
		TreeMap<Integer,String[]> most_likes= new TreeMap<Integer,String[]>();
		TreeMap<Integer,String[]> most_dislikes=new TreeMap<Integer,String[]>();

		// TODO 2: loop through values to find most viewed, most liked, and most disliked video
		for (Text value: values) {
			compositeString = value.toString();
			compositeStringArray = compositeString.split(IFS);
			temp_category_id = Integer.parseInt(compositeStringArray[0]);
			temp_views = Integer.parseInt(compositeStringArray[1]);
			temp_likes = Integer.parseInt(compositeStringArray[2]);
			temp_dislikes = Integer.parseInt(compositeStringArray[3]);

			// update most_views
			if (most_views == null || !most_views.containsKey(temp_category_id)) { 
				most_views.put(temp_category_id, compositeStringArray);
			}
			else if (Integer.parseInt(most_views.get(temp_category_id)[1]) < temp_views)
				most_views.put(temp_category_id, compositeStringArray);

			// update most_likes
			if (most_likes == null || !most_likes.containsKey(temp_category_id)) { 
				most_likes.put(temp_category_id, compositeStringArray);
			}
			else if (Integer.parseInt(most_likes.get(temp_category_id)[2]) < temp_likes)
				most_likes.put(temp_category_id, compositeStringArray);

			// update most_dislikes
			if (most_dislikes == null || !most_dislikes.containsKey(temp_category_id)) { 
				most_dislikes.put(temp_category_id, compositeStringArray);
			}
			else if (Integer.parseInt(most_dislikes.get(temp_category_id)[3]) < temp_dislikes)
				most_dislikes.put(temp_category_id, compositeStringArray);
		}

		// TODO 3: write the key-value pair to the context exactly as defined in lab write-up
		for(Integer category_id: most_views.keySet()) {
			String[] viewsArray = most_views.get(category_id);
			String[] likesArray = most_likes.get(category_id);
			String[] dislikesArray = most_dislikes.get(category_id);
			context.write(new Text("category_id: " + category_id), new Text("\nmost views: " + viewsArray[4] + OFS + viewsArray[5]
					+ "\nmost likes: " + likesArray[4] + OFS + likesArray[5] + "\nmost dislikes: " + dislikesArray[4] + OFS + dislikesArray[5]));
		}
	}
}
	