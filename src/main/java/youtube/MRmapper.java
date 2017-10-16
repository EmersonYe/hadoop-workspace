package youtube;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;

public class MRmapper  extends Mapper <LongWritable,Text,Text,Text> {
    static String IFS=","; // Internal Field Separator
    static String OFS=","; // Output Field Separator
    static int NF=11;
    
    public void map(LongWritable key, Text value, Context context) 
                    throws IOException, InterruptedException {
        
        /** USvideos.csv
        video_id
        title
        channel_title
        category_id
        tags
        views
        likes
        dislikes
        comment_total
        thumbnail_link
        date
        */
        
        // TODO 1: remove schema line
    if (key.get() == 0L) return;

        // TODO 2: convert value to string
    	String[] videoData = value.toString().split(IFS);

        // TODO 3: count num fields, increment bad record counter, and return if bad
    	if (videoData == null || videoData.length != NF) { // bad record
    		// increment bad record
    		return;
    	}
        // TODO 4: pull out fields of interest
    	String video_id = videoData[0];
    	String thumbnail_link = videoData[9];
    String category_id = videoData[3];
    String views = videoData[5];
    String likes = videoData[6];
    String dislikes = videoData[7];
    	
        // TODO 5: construct key and composite value 
    	Text newKey = new Text("summary");
    	Text compositeValue = new Text(category_id + OFS + views + OFS + likes + OFS + dislikes + OFS + video_id + OFS + thumbnail_link);
    	
        // TODO 6: write key value pair to context
    	context.write(newKey, compositeValue);
    }
}
