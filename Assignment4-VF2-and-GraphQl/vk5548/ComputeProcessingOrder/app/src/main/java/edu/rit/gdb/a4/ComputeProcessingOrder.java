package edu.rit.gdb.a4;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

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

public class ComputeProcessingOrder {

	public static void main(String[] args) throws Exception {
		final String selectedQuery = args[0];
		final String candidatesFile = args[1];
		final String gammaStr = args[2];
		final String resultsFolder = args[3];
		
		File selectedQueryFile = new File(selectedQuery);
		System.out.println(new Date() + " -- Query: " + selectedQueryFile.getName());
		System.out.println("selectedQuery : " + selectedQuery);
		System.out.println("candidatesFile : " + candidatesFile);
		System.out.println("gammaStr : " + gammaStr);
		System.out.println("resultsFolder : " + resultsFolder);
		String query = new String(Files.readAllBytes(Paths.get(selectedQueryFile.toURI())));
		Map<Long, Integer> candidateSizes = parseCandidateSizes(candidatesFile);
		List<Long> order = new ArrayList<>();
		try {
			// TODO Your code here!!!
			// Use this variable to build your query.
			Map<Long, SubGraph> subgraphQuery = new HashMap<>();
			// You have the query here!
			query.toString();

			//---------------------------------------------
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
			
			order = computeProcessingOrder(subgraphQuery, candidateSizes, Double.valueOf(gammaStr));
		} catch (Throwable oops) {
			// Awful issue!
			oops.printStackTrace();
		}
		PrintWriter writer = new PrintWriter(new File(resultsFolder + selectedQueryFile.getName() + "_Order.txt"));
		writer.println(order.toString());
		writer.close();
//		System.out.println(order.toString());
		
		System.out.println(new Date() + " -- Done");
	}
	
	private static Map<Long, Integer> parseCandidateSizes(String file) throws Exception {
		Map<Long, Integer> ret = new HashMap<>();
		Scanner sc = new Scanner(new File(file));
		while (sc.hasNextLine()) {
			String line = sc.nextLine().trim();
			if (line.length() == 0)
				continue;
			String[] qNodeAndSet = line.split(" ");
			ret.put(Long.valueOf(qNodeAndSet[0].trim()), Integer.valueOf(qNodeAndSet[1].trim()));
		}
		sc.close();
		return ret;
	}

	private static int getNumberOFConnections(List<Long> neighbors, List<Long> order){
		int result =0;
		for(Long n: neighbors){
			if(order.contains(n))
				result++;
		}
		return result;
	}
	
	private static List<Long> computeProcessingOrder(Map<Long, SubGraph> subgraphQuery, Map<Long, Integer> candidateSizes, double gamma) {
		List<Long> order = new ArrayList<>();
		// TODO Your code here!
		// The order must contain all query nodes. You must implement a greedy algorithm to select the best next query node
		//		to be processed. Use the cost prediction described in GraphQL.
		int  min = Integer.MAX_VALUE;
		Long uNext = null;
		for(Long u: candidateSizes.keySet()){
			int size = candidateSizes.get(u);
			if(size < min){
				uNext = u;
				min= size;
			}
		}
		//line 7
		order.add(uNext);
		double cost = Double.valueOf(min);
		double total = cost;
		//line 8 through 17
		double minimum = Double.MAX_VALUE;
		while(order.size() != candidateSizes.size()){
			uNext = null;
			minimum = Double.MAX_VALUE;
			for(Long u: candidateSizes.keySet()){
				if(!order.contains(u)){
					//get neighbors of u
					List<Long> neighbors = subgraphQuery.get(u).getNeighbors();
					int numOfConn = getNumberOFConnections(neighbors, order);
					double current = cost * candidateSizes.get(u) * Math.pow(gamma, numOfConn) ;
					if(current < minimum){
						uNext = u;
						minimum = current;
					}

				}
			}
			if(uNext != null){
				order.add(uNext);
				cost = minimum;
				total += cost;
			}

		}
		return order;
	}
	
	private static Set<Long> getQueryNodes(Map<Long, SubGraph> subgraphQuery) {
		// TODO Your code here!!!!
		// Get all the query nodes.
		return new HashSet<>();
	}
	
	private static int getDegree(Map<Long, SubGraph> subgraphQuery, List<Long> order, Long u) {
		// TODO Your code here!!!!
		// Get the degree between u and order in the query.
		return 0;
	}

}
