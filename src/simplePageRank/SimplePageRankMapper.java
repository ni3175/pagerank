package simplePageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SimplePageRankMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// value format: 
		// Node PageRank Residual Degree DstNodes
		String[] elems = value.toString().trim().split("\\s+");

		// Retrieve the information from the input
		Text srcNode = new Text(elems[0]);
		Float pageRank = new Float(elems[1]);
		Integer srcDegree = new Integer(elems[2]);

		if (srcDegree == 0) {
			// pass prevNodeInfo
			// srcNode -> "prevNodeInfo" pageRank srcDegree
			Text mapperKey = new Text(srcNode);
			Text mapperValue = new Text("prevNodeInfo "
					+ String.valueOf(pageRank) + " " + srcDegree);
			output.collect(mapperKey, mapperValue);
			return;
		}
		String dstNodesStr = elems[3];
		String[] dstNodes = dstNodesStr.split(",");

		// pass prevNodeInfo
		// srcNode -> "prevNodeInfo" pageRank srcDegree dstNodes
		Text mapperKey = new Text(srcNode);
		Text mapperValue = new Text("prevNodeInfo " + String.valueOf(pageRank)
				+ " " + srcDegree + " " + dstNodesStr);
		output.collect(mapperKey, mapperValue);

		// pass outgoing node
		// dstNode -> srcPageRank / srcDegree
		mapperValue = new Text(String.valueOf(pageRank / srcDegree));
		for (int i = 0; i < dstNodes.length; i++) {
			mapperKey = new Text(dstNodes[i]);
			output.collect(mapperKey, mapperValue);
		}
	}

}
