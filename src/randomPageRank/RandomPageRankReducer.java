package randomPageRank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RandomPageRankReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private HashMap<Integer, Float> oldPageRanks;
	private HashMap<Integer, Float> newPageRanks;
	private HashMap<Integer, Integer> nodeDegrees;
	private HashMap<Integer, ArrayList<Integer>> outNodes;

	private HashMap<Integer, ArrayList<Integer>> beMap;
	private HashMap<Integer, ArrayList<Float>> bcMap;
	
	private ArrayList<Integer> nodeList;
	
	// damping factor used to calculated the new pageRank
	private final static Float DAMPING_FACTOR = (float) 0.85;

	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		Float residualError = (float) 0.0;	
		
		// Initialize new hash map for each block
		oldPageRanks = new HashMap<Integer, Float>();
		newPageRanks = new HashMap<Integer, Float>();
		nodeDegrees = new HashMap<Integer, Integer>();
		outNodes = new HashMap<Integer, ArrayList<Integer>>();
		
		beMap = new HashMap<Integer, ArrayList<Integer>>();
		bcMap = new HashMap<Integer, ArrayList<Float>>();
		nodeList = new ArrayList<Integer>();
		int maxNode = 0;
				 
		while (values.hasNext()) {
			// incoming value format:
			// srcBlock -> "prevPageRank" srcNode pageRank srcDegree dstNodes
			// dstBlock -> "BE" srcNode srcDegree dstNodes
			// dstBlock -> "BC" srcNode srcDegree dstNodes pageRankFactor
			String input = values.next().toString();
			String[] elems = input.toString().trim().split("\\s+");

			// Processing the input information form the mapper
			if (elems[0].equals("prevNodeInfo")) {
				int srcNode = Integer.parseInt(elems[1]);
				Float oldPageRank = Float.parseFloat(elems[2]);
				int srcDegree = Integer.parseInt(elems[3]);
				// we need to keep track the old page rank for each node
				oldPageRanks.put(srcNode, oldPageRank);
				newPageRanks.put(srcNode, oldPageRank);
				// no outgoing list
				if (srcDegree == 0) {
					nodeDegrees.put(srcNode, 0);
					outNodes.put(srcNode, new ArrayList<Integer>());
				} else {
					nodeDegrees.put(srcNode, srcDegree);
					ArrayList<Integer> tempList = new ArrayList<Integer>();
					String[] dstNodes = elems[4].split(",");
					for (String dstNode : dstNodes)
						tempList.add(Integer.parseInt(dstNode));
					outNodes.put(srcNode, tempList);
				}
				nodeList.add(srcNode);
				// update the max number of node
				if (srcNode > maxNode)
					maxNode = srcNode;
			} else if (elems[0].equals("BE")) {
				// incoming edges from the same block
				// dstBlock -> "BE" srcNode srcDegree dstNode
				Integer srcNode = Integer.parseInt(elems[1]);
				Integer dstNode = Integer.parseInt(elems[2]);
				ArrayList<Integer> tempList;
				if (!beMap.containsKey(dstNode)) {
					tempList = new ArrayList<Integer>();
				} else {
					tempList = beMap.get(dstNode);
				}
				tempList.add(srcNode);
				beMap.put(dstNode, tempList);
			} else if (elems[0].equals("BC")) {
				// incoming edges from outside of the block
				// dstBlock -> "BC" srcNode srcDegree dstNode pageRankFactor
				Integer dstNode = Integer.parseInt(elems[1]);
				Float pageRankFactor = Float.parseFloat(elems[2]);
				ArrayList<Float> tempRList;
				if (!bcMap.containsKey(dstNode)) {
					tempRList = new ArrayList<Float>();
				} else {
					tempRList = bcMap.get(dstNode);
				}
				tempRList.add(pageRankFactor);
				bcMap.put(dstNode, tempRList);
			} else {
				// nothing to do
			}
		}

		// Iterate within each block until the value converged
		int counter = 0;
		float randomResidualError;
		do {
			randomResidualError = IteratorBlockOnce();
			counter++;
		} while (randomResidualError >= RandomPageRank.ERROR_BOUND);

		// output format
		// srcNode -> pageRank residualError srcDegree dstNodes
		for (Integer node : nodeList) {
			residualError = Math.abs(oldPageRanks.get(node) - newPageRanks.get(node))
					/ newPageRanks.get(node);
			String outputStr = newPageRanks.get(node)
					+ " " + nodeDegrees.get(node) + " " + StringUtils.join(outNodes.get(node), ',');
			Text outputKey = new Text(node.toString());
			Text outputValue = new Text(outputStr);
			output.collect(outputKey, outputValue);
			reporter.getCounter(RandomCounter.RESIDUAL_COUNTER).increment((long)(residualError * RandomPageRank.MULTIPLE));
			if (node == maxNode) {
				reporter.incrCounter("RandomCounter", "PAGERANK" + Integer.parseInt(key.toString()), 
						(long) (newPageRanks.get(node) * RandomPageRank.MULTIPLE));
			}
		}
		
		// increase reducer counter and inner iteration counter
		reporter.getCounter(RandomCounter.REDUCER_COUNTER).increment(1);
		reporter.getCounter(RandomCounter.INNER_ITERATION_COUNTER).increment(counter);
		
	}

	public float IteratorBlockOnce() {
		
		HashMap<Integer, Float> nextPageRanks = new HashMap<Integer, Float>();
		HashMap<Integer, Float> prevPageRanks = new HashMap<Integer, Float>();
		// Note : our nodeList is the Node v for v in B
		// for( v in B ) { NPR[v] = 0; }
		for (Integer node : nodeList) {
			prevPageRanks.put(node, newPageRanks.get(node));
		}
		// for( v in B ) {
		for (Integer node : nodeList) {
			Float nextPageRank = (float) 0.0;
			if (beMap.containsKey(node)) {
				// for( u where <u, v> in BE ) {
				for (Integer comingNode : beMap.get(node)) {
					// NPR[v] += PR[u] / deg(u);
					nextPageRank += (float) newPageRanks.get(comingNode) / nodeDegrees.get(comingNode);
				}
			}
			if (bcMap.containsKey(node)) {
				// for( u, R where <u,v,R> in BC ) {
				for (Float pageRankFactor : bcMap.get(node)) {
					// NPR[v] += R;
					nextPageRank += pageRankFactor;
				}
			}
			// NPR[v] = d*NPR[v] + (1-d)/N;
			nextPageRank = DAMPING_FACTOR * nextPageRank + (1 - DAMPING_FACTOR) / RandomPageRank.NUMBER_OF_NODES;
			nextPageRanks.put(node, nextPageRank);
		}

		// for( v in B ) { PR[v] = NPR[v]; }
		for (Integer node : nodeList) {
			newPageRanks.put(node, nextPageRanks.get(node));
		}
		float residualSum = (float) 0.0;
		for (Integer node : nodeList) {
			residualSum += Math.abs(prevPageRanks.get(node) - newPageRanks.get(node))
					/ newPageRanks.get(node);
		}
		residualSum /= nodeList.size();
		return residualSum;
	}

}
