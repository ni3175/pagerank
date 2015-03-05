package gaussPageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class GaussPageRankMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String[] elems = value.toString().trim().split("\\s+");

		// Retrieve the information from the input
		Integer srcNode = new Integer(elems[0]);
		Float pageRank = new Float(elems[1]);
		Integer srcDegree = new Integer(elems[2]);
		// get the block id
		Integer srcBlock = blockIDofNode(srcNode);
		Text mapperKey = new Text(srcBlock.toString());
		if (srcDegree == 0) {
			// pass prevNodeInfo
			// srcBlock -> "prevNodeInfo" srcNode pageRank srcDegree
			Text mapperValue = new Text("prevNodeInfo " + srcNode + " "
					+ String.valueOf(pageRank) + " "
					+ String.valueOf(srcDegree));
			output.collect(mapperKey, mapperValue);
			return;
		}
		String dstNodesStr = elems[3];
		String[] dstNodes = dstNodesStr.split(",");
		// srcBlock -> "prevNodeInfo" srcNode pageRank srcDegree dstNodes
		Text mapperValue = new Text("prevNodeInfo " + srcNode + " "
				+ String.valueOf(pageRank) + " " + String.valueOf(srcDegree) +
				" " + dstNodesStr);
		output.collect(mapperKey, mapperValue);
		// R = PR(u)/deg(u)
		Float pageRankFactor = (float) pageRank / srcDegree;
		for (int i = 0; i < dstNodes.length; i++) {
			Integer dstBlock = blockIDofNode(Integer.parseInt(dstNodes[i]));
			mapperKey = new Text(dstBlock.toString());
			if (dstBlock == srcBlock) {
				// BE = { <u,v> | u in B & u->v } = the Edges from Nodes in
				// Block B
				// dstBlock -> "BE" srcNode srcDegree dstNodes
				mapperValue = new Text("BE " + srcNode +  " " + dstNodes[i]);
			} else {
				// BC = { <u,v,R> | u not in B & v in B & u->v & R =
				// PR(u)/deg(u) } = the Boundary Conditions
				// dstBlock -> "BC" srcNode srcDegree dstNodes pageRankFactor
				mapperValue = new Text("BC " + dstNodes[i] + " " + pageRankFactor);
			}
			output.collect(mapperKey, mapperValue);
		}
	}
	
	private int blockIDofNode(int node) {
		int blockSize = 10000;
		int[] blockBoundaries = { 0, 10328, 20373, 30629, 40645,
				50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945,
				130999, 140574, 150953, 161332, 171154, 181514, 191625, 202004,
				212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473,
				293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929,
				374236, 384554, 394929, 404712, 414617, 424747, 434707, 444489,
				454285, 464398, 474196, 484050, 493968, 503752, 514131, 524510,
				534709, 545088, 555467, 565846, 576225, 586604, 596585, 606367,
				616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230 };
		int block = (int) Math.floor(node / blockSize);
		int boundary = blockBoundaries[block];
		if (node < boundary) {
			return block - 1;
		}
		return block;
	}

}
