package edu.rit.gdb.a4;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

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

public class PruneGlobally {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final String selectedQuery = args[1];
		final String target = args[2];
		final String resultsFolder = args[3];
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		Transaction tx = db.beginTx();
		
		File selectedQueryFile = new File(selectedQuery);
		System.out.println(new Date() + " -- Target: " + target + "; Query: " + selectedQueryFile.getName());
		System.out.println("----------------------------------");
		System.out.println("neo4jFolder : "+ neo4jFolder);
		System.out.println("selectedQuery : "+ selectedQuery);
		System.out.println("target : "+ target);
		System.out.println("resultsFolder : "+ resultsFolder);
		System.out.println("----------------------------------");
		String query = new String(Files.readAllBytes(Paths.get(selectedQueryFile.toURI())));
		Map<Long, Set<Long>> candidates = new HashMap<>();
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
			
			candidates = computeCandidates(subgraphQuery, tx);
		} catch (Throwable oops) {
			// Awful issue!
			oops.printStackTrace();
		}
		PrintWriter writer = new PrintWriter(new File(resultsFolder + target + "-" + selectedQueryFile.getName() + "_Candidates_Global.txt"));
		for (Long qid : candidates.keySet())
			writer.println(qid + " " + candidates.get(qid).size());
		writer.close();
		tx.close();
		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}
	
	private static Map<Long, Set<Long>> computeCandidates(Object subgraphQuery, Transaction tx) {
		Map<Long, Set<Long>> candidates = new HashMap<>();
		// Compute candidates, that is, nodes in the database that can be mapped to each node in the query.
		for (Long qid : getQueryNodes(subgraphQuery)) {
			Set<Long> dNodes = getMatchingNodes(subgraphQuery, qid, tx);
			Set<Long> finalDNodes = new HashSet<>();
			for (Long did : dNodes)
				finalDNodes.add(did);
			candidates.put(qid, finalDNodes);
		}
		pruneGlobally(subgraphQuery, tx, candidates);
		return candidates;
	}
	
	private static Set<Long> getQueryNodes(Object subgraphQuery) {
		// TODO Your code here!!!!
		// Get all the query nodes.
		return new HashSet<>();
	}
	
	private static Set<Long> getMatchingNodes(Object subgraphQuery, long qid, Transaction tx) {
		// TODO Your code here!!!!
		// Get the data nodes that match the current query node based on number of neighbors and labels.
		return new HashSet<>();
	}
	
	private static void pruneGlobally(Object subgraphQuery, Transaction tx, Map<Long, Set<Long>> candidates) {
		// TODO Your code here!!!!
		// Global pruning of the candidates. You can use any library for computing the a maximum cardinality bipartite matching
		//		or you can implement your own. If the latter, make sure your matching computation is correct before proceeding
		//		with the global pruning implementation.
	}

}
