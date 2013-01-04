import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class IncompletesMapper extends Mapper<LongWritable, Text, Text, PassWritable> {
	Logger logger = Logger.getLogger(IncompletesMapper.class);

	Pattern incompletePass = Pattern.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*pass.*to ([A-Z]*\\.\\s?[A-Za-z]*).*,(\\d*)$");
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		
		if (line.contains("incomplete")) {
			Matcher matcher = incompletePass.matcher(line);
			
			if (matcher.find()) {
				context.write(new Text(matcher.group(1) + "-" + matcher.group(2)), new PassWritable(1,Integer.parseInt(matcher.group(3))));
			} else {
				context.getCounter("inc", "notfound").increment(1);
				logger.warn("Did not match \"" + line + "\"");
			}
		}
	}
}