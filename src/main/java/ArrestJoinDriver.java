import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ArrestJoinDriver extends Configured implements Tool {

  public int run(String[] args) throws Exception {

    if (args.length != 3) {
      System.out.printf("Usage: ArrestJoinDriver <input dir> <output dir> <arrestfile>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(ArrestJoinDriver.class);
    job.setJobName("Arrest Joiner");
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    getConf().set("arrestfile", args[2]);
    
    job.setMapperClass(ArrestJoinMapper.class);
    job.setNumReduceTasks(0);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new ArrestJoinDriver(), args);
    System.exit(exitCode);
  }
}
