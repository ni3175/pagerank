package randomPageRank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class RandomPageRank {

	public final static int NUMBER_OF_NODES = 685230;
	public final static int NUMBER_OF_BLOCKS = 68;
	public final static long MULTIPLE = 100000000000L;
	
	public final static float ERROR_BOUND = (float) 0.001;
	public static int CURRENT_ITERATION = 0;

	public static void main(String[] args) throws Exception {
		// Specify the directory for the input and output file
		String inputFile = args[0] + "/input/zx78.txt";
		String outputPath = args[0] + "/output_random/";
		
		double avgResidualError = 0.0;
		long reducerCounter = 0;
		long innerIterationCounter = 0;
		
		do {
			int i = CURRENT_ITERATION;
			JobConf conf = new JobConf(RandomPageRank.class);
			
			// Set a unique job name
			conf.setJobName("randomPageRank_" + i);

			// Set Mapper and Reducer class for the page rank
			conf.setMapperClass(RandomPageRankMapper.class);
			conf.setReducerClass(RandomPageRankReducer.class);

			// set the classes for output key and value
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			// Setting the input path
			if (i == 0) {
				// 1st round, use the preprocessed file as the input file
				FileInputFormat.setInputPaths(conf, new Path(inputFile));
			} else {
				// otherwise use the output of the last pass as our input
				FileInputFormat.setInputPaths(conf, new Path(outputPath + "output" + (i-1)));
			}
			// set the output file path
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "output" + i));

			RunningJob rj = JobClient.runJob(conf);
			
			// read counter and reset
			avgResidualError = rj.getCounters().getCounter(RandomCounter.RESIDUAL_COUNTER) * 1.0/ MULTIPLE / NUMBER_OF_NODES;
			rj.getCounters().findCounter(RandomCounter.RESIDUAL_COUNTER).setValue(0);
			reducerCounter = rj.getCounters().getCounter(RandomCounter.REDUCER_COUNTER);
			rj.getCounters().findCounter(RandomCounter.REDUCER_COUNTER).setValue(0);
			innerIterationCounter = rj.getCounters().getCounter(RandomCounter.INNER_ITERATION_COUNTER);
			rj.getCounters().findCounter(RandomCounter.INNER_ITERATION_COUNTER).setValue(0);
			
			System.out.print("[pass " + i + "]  ");
			System.out.print("Avg. residual error: " + avgResidualError + ",  ");
			System.out.println("Avg. iteration: " + String.format("%.6f", innerIterationCounter * 1.0 / reducerCounter));
			
			CURRENT_ITERATION++;
			
			// output sample PR for every block
			if (avgResidualError < ERROR_BOUND) {
				System.out.println("\n");
				int remainder = (NUMBER_OF_NODES-1) % NUMBER_OF_BLOCKS;
				int quotient = (NUMBER_OF_NODES-1) / NUMBER_OF_BLOCKS;
				int base = quotient * NUMBER_OF_BLOCKS;
				for (int j = 0; j < NUMBER_OF_BLOCKS; j++) {
					int maxNode = (j <= remainder ? base + j : base - NUMBER_OF_BLOCKS + j);
					System.out.println("[block " + j + "]  Max node: " + 
							maxNode + "  PageRank: " + 
							rj.getCounters().findCounter("RandomCounter", "PAGERANK" + j).getValue() * 1.0 / MULTIPLE);
				}
			}
			
		} while (avgResidualError >= ERROR_BOUND);
		
	}
}
