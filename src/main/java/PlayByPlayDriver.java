import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;

import com.jesseanderson.data.Play;

public class PlayByPlayDriver extends Configured implements Tool {
	private static final String PHASE_ONE_MR = "phaseone";

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out
					.printf("Usage: PlayByPlayDriver <input dir> <output dir>\n");
			return -1;
		}

		Job job = Job.getInstance(getConf());
		job.setJarByClass(PlayByPlayDriver.class);
		job.setJobName("Play by Play parser");

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setMapperClass(PlayByPlayMapper.class);
		job.setReducerClass(GameWinnerReducer.class);

		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setMapOutputValueSchema(job, Play.getClassSchema());
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Play.class);
		AvroJob.setOutputKeySchema(job, Play.getClassSchema());
		
		String repositoryUri = "repo:hive:" + args[1];

		job.setOutputFormatClass(DatasetKeyOutputFormat.class);
		job.getConfiguration().set(DatasetKeyOutputFormat.KITE_DATASET_NAME,
				PHASE_ONE_MR);
		job.getConfiguration().set(DatasetKeyOutputFormat.KITE_REPOSITORY_URI,
				repositoryUri);

		// Create the repository object in Hive Metastore
		DatasetRepository repo = DatasetRepositories.open(repositoryUri);

		if (repo.exists(PHASE_ONE_MR)) {
			// Delete the repo if it already exists
			// NOTE: You wouldn't do this for production code!!!
			repo.delete(PHASE_ONE_MR);
		}

		// Create the repository using the Avro schema and partition
		DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
				.schemaUri("resource:play.avsc").build();
		repo.create(PHASE_ONE_MR, descriptor);
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new PlayByPlayDriver(), args);
		System.exit(exitCode);
	}
}
