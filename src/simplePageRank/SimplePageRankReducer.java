package simplePageRank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SimplePageRankReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	// damping factor used to calculated the new pageRank
	private final static Float DAMPING_FACTOR = (float) 0.85;
	
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// new page rank
		Float newPageRank = (float) 0.0;
		// old page rank
		Float oldPageRank = (float) 0.0;
		// value used to calculate the new page rank
		Float pageRankSum = (float) 0.0;

		// list used to store the dst nodes
		ArrayList<Integer> dstNodes = new ArrayList<Integer>();

		while (values.hasNext()) {
			// incoming value format:			
			// srcNode -> "prevNodeInfo" pageRank srcDegree dstNodes
			// dstNode -> srcPageRank / srcDegree
			String input = values.next().toString();	
			String[] elems = input.toString().trim().split("\\s+");
			
			// two types of input messages from mapper
			if (elems[0].equals("prevNodeInfo")) {
				// srcNode -> "prevNodeInfo" pageRank srcDegree dstNodes
				oldPageRank = Float.parseFloat(elems[1]);
				Integer srcDegree = Integer.parseInt(elems[2]);
				if (srcDegree != 0){
					String[] dstNodesStr = elems[3].split(",");
					for (String dstNode : dstNodesStr)
						dstNodes.add(Integer.parseInt(dstNode));
				}	
			} else {
				// dstNode -> srcPageRank / srcDegree
				pageRankSum += Float.parseFloat(elems[0]);
			}
		}

		// Compute new pageRank:
		// newPageRank = dampingFactor * (sum(pageRank[v]/degrees[v])) + (1 - dampingFactor)/N
		// random jump factor = (1 - DF) / N
		newPageRank = (float)(DAMPING_FACTOR * pageRankSum) + ((float) (1 - DAMPING_FACTOR) / SimplePageRank.NUMBER_OF_NODES);

		// compute the residual error for this node
		Float residual = (float) Math.abs(oldPageRank - newPageRank) / newPageRank;
		
		// update redisual counter
		reporter.getCounter(Counter.RESIDUAL_COUNTER).increment((long)(residual * SimplePageRank.MULTIPLE));

		// output format:
		// NodeID -> pageRankNew residualError srcDegree dstNodes
		Integer srcDegree = new Integer(dstNodes.size());
		Text reducerValue = new Text(newPageRank + " " + srcDegree.toString() + " " + StringUtils.join(dstNodes, ','));

		output.collect(key, reducerValue);
	}

}
