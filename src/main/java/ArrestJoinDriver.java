import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;

import com.jesseanderson.data.Play;

public class ArrestJoinDriver extends Configured implements Tool {
	private static final String ARREST_JOIN_MR = "arrestjoin";

	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.printf("Usage: ArrestJoinDriver <input dir> <output dir> <arrestfile>\n");
			return -1;
		}

		Job job = Job.getInstance(getConf());
		job.setJarByClass(ArrestJoinDriver.class);
		job.setJobName("Arrest Joiner");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		getConf().set("arrestfile", args[2]);

		job.setMapperClass(ArrestJoinMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setMapOutputValueSchema(job, Play.getClassSchema());

		job.setOutputKeyClass(Play.class);
		AvroJob.setOutputKeySchema(job, Play.getClassSchema());
		AvroJob.setInputKeySchema(job, Play.getClassSchema());

		String inputRepositoryUri = args[0];
		String outputRepositoryUri = args[1];

		System.out.println("Input:" + inputRepositoryUri + " Output:" + outputRepositoryUri);

		job.setInputFormatClass(DatasetKeyInputFormat.class);
		job.getConfiguration().set(DatasetKeyInputFormat.KITE_DATASET_NAME, PlayByPlayDriver.PHASE_ONE_MR);
		job.getConfiguration().set(DatasetKeyInputFormat.KITE_REPOSITORY_URI, inputRepositoryUri);

		job.setOutputFormatClass(DatasetKeyOutputFormat.class);
		job.getConfiguration().set(DatasetKeyOutputFormat.KITE_DATASET_NAME, ARREST_JOIN_MR);
		job.getConfiguration().set(DatasetKeyOutputFormat.KITE_REPOSITORY_URI, outputRepositoryUri);
		
		// Create the repository object in Hive Metastore
		DatasetRepository repo = DatasetRepositories.open(outputRepositoryUri);

		if (repo.exists(ARREST_JOIN_MR)) {
			// Delete the repo if it already exists
			// NOTE: You wouldn't do this for production code!!!
			repo.delete(ARREST_JOIN_MR);
		}

		// Create the repository using the Avro schema and partition
		DatasetDescriptor descriptor = new DatasetDescriptor.Builder().schemaUri("resource:play.avsc").build();
		repo.create(ARREST_JOIN_MR, descriptor);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new ArrestJoinDriver(), args);
		System.exit(exitCode);
	}
}
