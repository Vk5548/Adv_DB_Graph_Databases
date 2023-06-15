package edu.rit.gdb.a5;

import org.apache.commons.collections.keyvalue.AbstractMapEntry;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

class SubGraph{
	private Long id;
	private List<Long> neighbors;
	private String label;
	private String[] profile;

	SubGraph(Long id, String label, List<Long> neighbors){
		this.id = id;
		this.neighbors = neighbors;
		this.label = label;
	}

	public String getLabel(){
		return this.label;
	}

	public List<Long> getNeighbors(){
		return this.neighbors;
	}

	public boolean ifNeighbor(SubGraph obj2){
		return this.neighbors.contains(obj2.id)?true:false;
	}
}

public class BuildDAG {

	public static void main(String[] args) throws Exception {
		final String selectedQuery = args[0];
		final String processingOrder = args[1];
		final String resultsFolder = args[2];
		

//
		List<Long> order = Arrays.asList(processingOrder.replace("[", "").replace("]", "").split(", ")).stream().map(u->Long.valueOf(u)).collect(Collectors.toList());
//		List<Long> order = new ArrayList<>();
//		[6, 4, 3, 5, 2, 1, 0, 7]
//		order.add(Long.valueOf(6));
//		order.add(Long.valueOf(4));
//		order.add(Long.valueOf(3));
//		order.add(Long.valueOf(5));
//		order.add(Long.valueOf(2));
//		order.add(Long.valueOf(1));
//		order.add(Long.valueOf(0));
//		order.add(Long.valueOf(7));

		List<Long> modifiedOrder = new ArrayList<>();
		for(Long n : order){
			modifiedOrder.add(n);
		}
//		System.out.println(" ------------- ---- "+order  + " -------------");
		File selectedQueryFile = new File(selectedQuery);
		System.out.println(new Date() + " -- Query: " + selectedQueryFile.getName());
		String query = new String(Files.readAllBytes(Paths.get(selectedQueryFile.toURI())));
//		System.out.println("selectedQuery : " + selectedQuery);
//		System.out.println("processingOrder : " + processingOrder);
//		System.out.println("resultsFolder : " + resultsFolder);


		//---------------------------------------------

		// Use this variable to build your query.
		Map<Long, SubGraph> subgraphQuery = new HashMap<>();
		//PROCESSING THE STRING
		//nodes in this list
		List<Long> nodes = new ArrayList<>();
		//hashmap to store node and its neighbors
		Map<Long, String> labels = new HashMap<>();

		StringReader sr = new StringReader(query);
		BufferedReader br = new BufferedReader(sr);
		int numberOfNodes = Integer.parseInt(br.readLine());

		for(int i =1; i <= numberOfNodes; i++){
			String[] nodeWithLabels = br.readLine().split(" ");

			nodes.add(Long.valueOf(nodeWithLabels[0]));
			labels.put(Long.valueOf(nodeWithLabels[0]), nodeWithLabels[1]); //LABELS HASHMAP
		}
		//processing neighbors:
		Map<Long, List<Long>> neighbors = new HashMap<>(); //NEIGHBORS
		String relation;
		while((relation=br.readLine()) != null){

			String[] rel = relation.split(" ");
			if(rel.length == 1){
				continue;
			}
			if(rel.length == 2){
				Long node1 = Long.valueOf(rel[0]);
				Long node2 = Long.valueOf(rel[1]);

				List<Long> ls;
				if(neighbors.containsKey(node1)){
					ls = neighbors.get(node1);
					if(!ls.contains(node2))
						ls.add(node2);
				} else{
					ls = new ArrayList<>();
					ls.add(node2);
				}
				neighbors.put(node1, ls);
				if(neighbors.containsKey(node2)){
					ls = neighbors.get(node2);
					if(!ls.contains(node1))
						ls.add(node1);

				}else{
					ls = new ArrayList<>();
					ls.add(node1);
				}
				neighbors.put(node2, ls);
			}
		}
		// BUILDING SUBGRAPH
		for(Long id: labels.keySet()){
			//get label
			String label = labels.get(id);
			//get neighbors
			List<Long> neighborOfOneNode = neighbors.get(id);
			SubGraph obj = new SubGraph(id, label, neighborOfOneNode);
			subgraphQuery.put(id, obj);
		}

		//RELEASING ALL THE MEMORY WHICH WON'T BE USED
		labels = null;
		neighbors = null;
		nodes = null;


		//---------------------------------------------
		// We will use map entries as pairs. The key of an entry represents the current node and the value
		// represents its parent.
		//	The root has no parent, i.e., value is -1. Use Map.entry(key, value) to create an entry.
		List<Map.Entry<Long, Long>> output = new ArrayList<>();
		//addExpected("0>-1,1>0,2>1,7>1,3>2,4>3,5>4,6>4,");

//		order.add("[6, 4, 3, 5, 2, 1, 0, 7]");
//		addExpected("6>-1,0>1,7>1,1>2,2>3,3>4,5>4,4>6,");
		try {
			output.add(new AbstractMap.SimpleEntry<Long, Long>(order.get(0), Long.valueOf(-1)));
			for(int i = 1; i < order.size(); i++){
//				here:
//				{
					Long current = order.get(i);
//					for (Map.Entry<Long, Long> tuple : output) {
//						long key = tuple.getKey();
//						if (key == current)
//							break here;
//					}
					List<Long> possibleParents = subgraphQuery.get(current).getNeighbors();
					for(Long n: possibleParents){
						if(modifiedOrder.indexOf(n) < modifiedOrder.indexOf(current)){
							output.add(new AbstractMap.SimpleEntry<Long, Long>(current, n));
						}
					}
//				}

			}
			// TODO Your code here!!!
			// The direction of the DAG must be the same as in the processing order. The root is the first node in the order.
			//		The direction of the edges is decided based on the positions of the nodes in the order.

		} catch (Throwable oops) {
			// Awful issue!
			oops.printStackTrace();
		}
		
		PrintWriter writer = new PrintWriter(new File(resultsFolder + selectedQueryFile.getName() + order.get(0) + "-DAG.txt"));
		// Sort output by parent.
		Collections.sort(output, new Comparator<Map.Entry<Long, Long>>() {
			@Override
			public int compare(Entry<Long, Long> entry, Entry<Long, Long> other) {
				int cmp = entry.getValue().compareTo(other.getValue());
				if (cmp == 0)
					cmp = entry.getKey().compareTo(other.getKey());
				return cmp;
			}
		});
		for (Map.Entry<Long, Long> entry : output){
			writer.println(entry.getKey() + ">" + entry.getValue());
//			System.out.println(entry.getKey() + ">" + entry.getValue());
		}

		writer.close();
		
		System.out.println(new Date() + " -- Done");
	}

}
