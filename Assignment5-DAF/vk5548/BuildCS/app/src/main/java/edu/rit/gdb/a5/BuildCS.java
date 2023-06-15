package edu.rit.gdb.a5;

import java.io.BufferedReader;
import java.io.File;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
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

public class BuildCS {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final String selectedQuery = args[1];
		final String target = args[2];
		final String dagStr = args[3];
		final String candidatesFolder = args[4];
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		Transaction tx = db.beginTx();
		
		File selectedQueryFile = new File(selectedQuery);
		System.out.println(new Date() + " -- Query: " + selectedQueryFile.getName());
		System.out.println("neo4jFolder : "+ neo4jFolder );
		System.out.println("selectedQuery : "+ selectedQuery );
		System.out.println("target : "+ target );
		System.out.println("dagStr : "+ dagStr );
		System.out.println("candidatesFolder : "+ candidatesFolder );
		String query = new String(Files.readAllBytes(Paths.get(selectedQueryFile.toURI())));

		System.out.println("--------------------------------------");
		System.out.println("query \n: "+ query);
		System.out.println("--------------------------------------");
		
		List<Map.Entry<Long, Long>> dag = Arrays.stream(dagStr.substring(0, dagStr.length()-1).split(",")).
				map(e->Map.entry(Long.valueOf(e.substring(0, e.indexOf(">"))), Long.valueOf(e.substring(e.indexOf(">")+1, e.length())))).collect(Collectors.toList());
		
		File candidatesFile = new File(candidatesFolder + target + "-" + selectedQueryFile.getName() + "-Candidates.txt");
		List<String> candidatesStr = Files.readAllLines(Paths.get(candidatesFile.toURI()));
		Map<Long, Set<Long>> candidates = new HashMap<>();
		System.out.println("--------------------------------------");
		System.out.println("candidatesStr \n: "+ candidatesStr);
		System.out.println("--------------------------------------");
		for (String candStr : candidatesStr) {
			String[] queryAndData = candStr.split(" ");
			long u = Long.valueOf(queryAndData[0]);
			Set<Long> C = new HashSet<>();
			if (queryAndData.length > 1)
				for (String v : queryAndData[1].split(","))
					C.add(Long.valueOf(v));
			candidates.put(u, C);
		}

		candidates = getInternalIDS(candidates, target, tx);

		System.out.println("--------------------------------------");
		System.out.println("candidates :" + candidates);
		System.out.println("--------------------------------------");
		// TODO Note that the candidate ids are the actual ids, not the internal ids used by Neo4j.
		//	If you need the internal ids, then you need to map from the ids in the map with the 
		//	internal ids in the database.
		//for each node
		// Match u : targetDB
		// where u.id = $id
		//return id(u)

		// TODO YOU NEED TO STORE INTERNAL IDS INSTEAD
		
		// Use this variable to build your query.
		Map<Long, SubGraph> subgraphQuery = new HashMap<>();
		
		try {
			// TODO Your code here!!!
			// Parse the query.

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


			// Refine CS.
			refineCS(subgraphQuery, tx, candidates, dag);
		} catch (Throwable oops) {
			// Awful issue!
			oops.printStackTrace();
		}
		
		tx.close();
		
		if (subgraphQuery != null) {
			for (Map.Entry<Long, Long> entry : dag) {
				tx = db.beginTx();
				
				// TODO Your code here!!!
				// Materialize candidates: Having (up, u), we generate a directed edge from vp in C(up) to v in C(u) with type 'up-u'.

				
				
				// TODO End of your code!
				tx.commit();
				tx.close();
			}
			
			tx = db.beginTx();
			
			tx.close();
		}
		
		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}
	
	private static Set<Long> getQueryNodes(Map<Long, SubGraph> subgraphQuery) {
		// TODO Your code here!!!!
		// Get all the query nodes.
		Set<Long> queryNodes = new HashSet<>();
		for(Long u: subgraphQuery.keySet()){
			queryNodes.add(u);
		}
		return queryNodes;
	}

	private static Map<Long, Set<Long>> getInternalIDS(Map<Long, Set<Long>> candidates, String target, Transaction tx){
		//iterate through the all the nodes and check if the node is present
		//or have them in a single query
		Map<Long, Set<Long>> candidatesWithID = new HashMap<>();
		String cypher = "MATCH (u:`"+target+"`) \n" +
				"WHERE u.id = $id \n" +
				"RETURN id(u) as id;";
		Map<String,Object> params = new HashMap<>();

		for(Long u: candidates.keySet()){
			Set<Long> potentialVS = candidates.get(u);
			Set<Long> potentialVWithIDS = new HashSet<>();
			for(Long v: potentialVS){
				params = new HashMap<>();
				params.put("id", v);
				Result result = tx.execute( cypher, params );
				while(result.hasNext()){
					Map<String, Object> hm = result.next();
					potentialVWithIDS.add(Long.valueOf("" + hm.get("id")));
				}

			}
			candidatesWithID.put(u, potentialVWithIDS);
		}
		return  candidatesWithID;
	}

	private static List<Map.Entry<Long, Long>> createReverseDAG(List<Map.Entry<Long, Long>> dag, Map<Long, SubGraph> subgraphQuery){
		List<Map.Entry<Long, Long>> revDAG = new ArrayList<>();
		for(Map.Entry<Long, Long> tuple : dag){
			here: {
				Long key = tuple.getKey();
				Long val = tuple.getValue();
				if(val != -1){
					for(Map.Entry<Long, Long> existing : revDAG){
						Long keyOld = existing.getKey();
						Long valNew = existing.getValue();
						if(val == keyOld){
							break here;
						}
					}
					revDAG.add(new AbstractMap.SimpleEntry<>(val, key));
				}
			}

		}
		Set<Long> queryNodes = getQueryNodes(subgraphQuery);
		if(revDAG.size() != dag.size()){
			boolean flag = false;
			for(Long u: queryNodes){
				flag = false;
				for(Map.Entry<Long, Long> existing : revDAG){
					Long key = existing.getKey();

					if(key == u){
						flag = true;
						break;
					}
				}
				if(!flag){

					revDAG.add(new AbstractMap.SimpleEntry<>(u, Long.valueOf(-1)));
				}
			}

		}
		return revDAG;
	}
	
	private static void refineCS(Map<Long, SubGraph> subgraphQuery, Transaction tx, Map<Long, Set<Long>> candidates, List<Map.Entry<Long, Long>> dag) {
		// We will implement the same process as pruning globally. However, we will take the DAG into account now.
		// We will alternate regular and reverse processing of the DAG. If not regular, the candidates of a query node
		// are refined only if all its children have been refined.
		// If reverse, the candidates of a query node are refined only if all its parents have been refined.
		//Let P(u) be the query nodes that are related to u (children if regular, parents if reverse).
		// The refinement of the candidates of u is as follows: v in C(u) iff There is a semi-perfect matching
		// between P(u) and N(v).
		//	If this is not the case, remove v as a candidate of u.
		List<Map.Entry<Long, Long>> revDAG = createReverseDAG(dag, subgraphQuery);
		boolean isRefined = false;
		int count = 0;
		
		do {
			isRefined = false;
			// We will start with regular, then reverse, then regular, then reverse...
			boolean reverse = count % 2 != 0;
			
			// For each query node, nodes that must be checked (children or parents depending on whether regular or reverse).
			Map<Long, Set<Long>> toCheck = new HashMap<>();
			Set<Long> queryNodes = getQueryNodes(subgraphQuery);
			
			for (Long u : queryNodes)
				toCheck.put(u, new HashSet<>());
			
			// TODO Your code here!!!!
			// Load toCheck with the appropriate values.
			
			
			// Let's process each query node.
			Set<Long> processed = new HashSet<>();
			while (processed.size() < queryNodes.size()) {
				for (Long u : queryNodes)
					// We will process u only if those that must be checked were processed.
					if (processed.containsAll(toCheck.get(u))) { // WHAT THE HELL
						processed.add(u);
						
						for (Long v : new HashSet<>(candidates.get(u))) {
							// TODO Your code here!!!
							// Build bipartite and check candidates. If not, refine.
							
							
						}
					}
			}
			
			count++;
		} while (isRefined);
	}

}
