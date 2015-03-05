package simplePageRank;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class SimplePageRank {

	public final static int NUMBER_OF_NODES = 685230;
	public final static int MULTIPLE = 1000000;
	
	private final static int NUM_ITERATIONS = 6; // # of iterations to run
	
	public static void main(String[] args) throws Exception {

		// Specify the directory for the input and output file
		String inputFile = args[0] + "/input/zx78.txt";
		String outputPath = args[0] + "/output_simple/";
		
		System.out.println("Running page rank on preprocessed file");
		for (int i = 0; i < NUM_ITERATIONS; i++) {
			
			// Start a JobConf
			JobConf conf = new JobConf(SimplePageRank.class);
			
			// Set a unique job name
			conf.setJobName("simplePageRank_" + i);

			// Set Mapper and Reducer class for the page rank
			conf.setMapperClass(SimplePageRankMapper.class);
			conf.setReducerClass(SimplePageRankReducer.class);

			// set the classes for output key and value
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			// Setting the input path
			if (i == 0) {
				// 1st round, use the pre-processed file as the input file
				FileInputFormat.setInputPaths(conf, new Path(inputFile));
			} else {
				// otherwise use the output of the last pass as our input
				FileInputFormat.setInputPaths(conf, new Path("output" + (i-1)));
			}
			
			// set the output file path
			if (i != NUM_ITERATIONS - 1)  {
				FileSystem fs = FileSystem.get(conf);
				// delete if exists
				if (fs.exists((new Path("output" + i))))
					fs.delete(new Path("output" + i), true); 
				FileOutputFormat.setOutputPath(conf, new Path("output" + i));
			}
			else
				FileOutputFormat.setOutputPath(conf, new Path(outputPath + "output" + i));

			// run job
			RunningJob rj = JobClient.runJob(conf);
			
			// get counter value and print
			double resVal = rj.getCounters().getCounter(Counter.RESIDUAL_COUNTER) * 1.0 / MULTIPLE / NUMBER_OF_NODES;
			System.out.println("Iteration " + i +"  Avg. residual error: " + resVal);
			
		}
		
		System.out.println("Page rank finished");
	}
}
