import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, PassWritable, Text, PassWritable> {
	HashSet<Integer> seasons = new HashSet<Integer>();
	
	@Override
	public void reduce(Text key, Iterable<PassWritable> values, Context context) throws IOException, InterruptedException {
		seasons.clear();
		
		int total = 0;
		
		for (PassWritable value : values) {
			total++;
			seasons.add(value.season);
		}
		
		context.write(key, new PassWritable(total, seasons.size()));
	}
}