package gaussPageRank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class GaussPageRank {

	public final static int NUMBER_OF_NODES = 685230;
	public final static int NUMBER_OF_BLOCKS = 68;
	public final static long MULTIPLE = 100000000000L;
	
	public final static float ERROR_BOUND = (float) 0.001;
	public static int CURRENT_ITERATION = 0;

	// hard code the boundary of blocks
	public static int[] blockBoundary = { 10328, 20373, 30629, 40645,
			50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945,
			130999, 140574, 150953, 161332, 171154, 181514, 191625, 202004,
			212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473,
			293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929,
			374236, 384554, 394929, 404712, 414617, 424747, 434707, 444489,
			454285, 464398, 474196, 484050, 493968, 503752, 514131, 524510,
			534709, 545088, 555467, 565846, 576225, 586604, 596585, 606367,
			616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230 };

	
	public static void main(String[] args) throws Exception {
		// Specify the directory for the input and output file
		String inputFile = args[0] + "/input/zx78.txt";
		String outputPath = args[0] + "/output_gauss/";
		
		double avgResidualError = 0.0;
		long reducerCounter = 0;
		long innerIterationCounter = 0;
		
		do {
			int i = CURRENT_ITERATION;
			JobConf conf = new JobConf(GaussPageRank.class);
			
			// Set a unique job name
			conf.setJobName("gaussPageRank_" + i);

			// Set Mapper and Reducer class for the page rank
			conf.setMapperClass(GaussPageRankMapper.class);
			conf.setReducerClass(GaussPageRankReducer.class);

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
			avgResidualError = rj.getCounters().getCounter(GaussCounter.RESIDUAL_COUNTER) * 1.0/ MULTIPLE / NUMBER_OF_NODES;
			rj.getCounters().findCounter(GaussCounter.RESIDUAL_COUNTER).setValue(0);
			reducerCounter = rj.getCounters().getCounter(GaussCounter.REDUCER_COUNTER);
			rj.getCounters().findCounter(GaussCounter.REDUCER_COUNTER).setValue(0);
			innerIterationCounter = rj.getCounters().getCounter(GaussCounter.INNER_ITERATION_COUNTER);
			rj.getCounters().findCounter(GaussCounter.INNER_ITERATION_COUNTER).setValue(0);
			
			System.out.print("[pass " + i + "]  ");
			System.out.print("Avg. residual error: " + avgResidualError + ",  ");
			System.out.println("Avg. iteration: " + String.format("%.6f", innerIterationCounter * 1.0 / reducerCounter));
			
			CURRENT_ITERATION++;
			
			// output sample PR for every block
			if (avgResidualError < ERROR_BOUND) {
				System.out.println("\n");
				for (int j = 0; j < NUMBER_OF_BLOCKS; j++) 
					System.out.println("[block " + j + "]  Max node: " + String.valueOf(blockBoundary[j]-1) + "  PageRank: " + 
							rj.getCounters().findCounter("BlockCounter", "PAGERANK" + j).getValue() * 1.0 / MULTIPLE);
			}
			
		} while (avgResidualError >= ERROR_BOUND);
		
	}
}
