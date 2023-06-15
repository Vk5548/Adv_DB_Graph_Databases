package edu.rit.gdb.a4;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

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

public class GraphQLSubgraph {
	
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0],
				proteinsFolder = args[1],
				listOfTargets = args[2],
				listOfQueries = args[3];
		final boolean induced = Boolean.valueOf(args[4]);
		final String gammaStr = args[5];
		final String resultsFolder = args[6];
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		Transaction tx = db.beginTx();
		
		Map<String, File> allQueryFiles = new HashMap<>();
		for (File q : new File(proteinsFolder + "query/").listFiles())
			allQueryFiles.put(q.getName(), q);
		for (String target : listOfTargets.split(","))
			for (String selectedQuery : listOfQueries.split(",")) {
				System.out.println(new Date() + " -- Target: " + target + "; Query: " + selectedQuery);
				System.out.println("------------------------------------");
				System.out.println("neo4jFolder : " + neo4jFolder);
				System.out.println("proteinsFolder : " + proteinsFolder);
				System.out.println("listOfTargets : " + listOfTargets);
				System.out.println("listOfQueries : " + listOfQueries);
				System.out.println("induced : " + induced);
				System.out.println("gammaStr : " + gammaStr);
				System.out.println("resultsFolder : " + resultsFolder);
				System.out.println("------------------------------------");
				String query = new String(Files.readAllBytes(Paths.get(allQueryFiles.get(selectedQuery).toURI())));
				List<Map<Long, Long>> results = new ArrayList<>();
				try {
					results = matching(tx, query, target, induced, gammaStr);
				} catch (Throwable oops) {
					// Awful issue!
					oops.printStackTrace();
				}
				PrintWriter writer = new PrintWriter(new File(resultsFolder + target + "-" + selectedQuery + "-" + induced + "_Solutions.txt"));
				for (Map<Long, Long> sol : results)
//					System.out.println(sol);
					writer.println(sol);
				writer.close();
				System.out.println(new Date() + " -- Done");
			}
		tx.close();
		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}
	
	public static List<Map<Long, Long>> matching(Transaction tx, String query, String targetDb, boolean isInduced, String gammaStr) throws IOException {
		List<Map<Long, Long>> result = new ArrayList<>();
		
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


		// Perform subgraph isomorphism.
		// The query must return the original id (not the internal, the property id) of each identified node.
		// Recall that to compute induced subgraph matching you must check that, if there is no edge between
		//		two query nodes, there must be no edge between the mapped data nodes.
		// The subgraphQuery object will contain a graph structure to store the query at hand. Generate this 
		//		query from the file once and store it in memory. Recall that you must deal with labels and
		//		neighbors, so your graph structure should support this. You can use your own in-memory data
		//		structure or use any external library for this.
		
		// TODO End of your code.
		
		Map<Long, Set<Long>> candidates = computeCandidates(subgraphQuery, tx, targetDb);

		Map<Long, Integer> candidateSizes = calculateSize(candidates);
		
		List<Long> order = computeProcessingOrder(subgraphQuery, candidateSizes, Double.valueOf(gammaStr));
		
		subgraphIsomorphism(subgraphQuery, tx, candidates, order, 0, new HashMap<>(), result, isInduced);
		//-----------------------------------------------------------------------------
		//getting attribute ids instead of internal ids
		List<Map<Long, Long>> results = new ArrayList<>();
		Map<Long, Long> newMapping = new HashMap<>();
		for(Map<Long, Long> mapping: result){ //result is arraylist of mapping
			newMapping = new HashMap<>();
			for(Long u: mapping.keySet()){ //runs 7 times

				Long v = mapping.get(u);
				String cypher = "MATCH (u)\n" +
						"WHERE id(u) = $id\n" +
						"RETURN u.id";
				Map<String,Object> params = new HashMap<>();
				params.put("id", v);
				Result result1 = tx.execute(cypher, params);
				Map<String, Object> map = null;
				if(result1.hasNext()){
					map = result1.next();
					Long x =Long.valueOf("" +map.get("u.id"));
					newMapping.put(u, x);

				}

				result1.close();
//				System.out.println(map.toString());
//				Long x = Long.valueOf("" + tx.getNodeById(mapping.get(v)).getProperty("id")) ;
//				if(x != null)
//					mapping.put(u, x);
			}
			results.add(newMapping);
		}
		//-----------------------------------------------------------------------------
		
		return results;
	}
	
	private static Map<Long, Set<Long>> computeCandidates(Map<Long, SubGraph> subgraphQuery, Transaction tx, String target) {
		Map<Long, Set<Long>> candidates = new HashMap<>();
		// Compute candidates, that is, nodes in the database that can be mapped to each node in the query. Use profiles.
		for (Long qid : getQueryNodes(subgraphQuery)) {
			String[] qPrf = getQueryNodeProfile(subgraphQuery, qid);
			Set<Long> dNodes = getMatchingNodes(subgraphQuery, qid, tx, target);
			Set<Long> finalDNodes = new HashSet<>();
			for (Long did : dNodes)
				if (!prune(qPrf, did, tx))
					finalDNodes.add(did);
			candidates.put(qid, finalDNodes);
		}
		// Global pruning.
		pruneGlobally(subgraphQuery, tx, candidates);
		return candidates;
	}

	private static Set<Long> getQueryNodes(Map<Long, SubGraph>  subgraphQuery) {
		// TODO Your code here!!!!
		// Get all the query nodes.
		Set<Long> result = new HashSet<>();
		for(Long id: subgraphQuery.keySet()){
			result.add(id);
		}
		return result;
	}

	private static String[] getQueryNodeProfile(Map<Long, SubGraph> subgraphQuery, long qid) {
		// TODO Your code here!!!!
		List<String> prf = new ArrayList<>();
		String[] profile = null;
		// Get the profile of the query node.
		SubGraph obj = subgraphQuery.get(qid);
		//get neighbors
		List<Long> neighbors = obj.getNeighbors();
		//getting labels of neighbors
		for(Long id: neighbors){
			SubGraph o = subgraphQuery.get(id);
			prf.add(o.getLabel());
		}
		profile = new String[prf.size()];
		for(int i =0; i < prf.size(); i++){
			profile[i] = prf.get(i);
		}
		Arrays.sort(profile);
		return profile;
	}

	private static Set<Long> getMatchingNodes(Map<Long, SubGraph> subgraphQuery, long qid, Transaction tx, String target) {
		// TODO Your code here!!!!
		// Get the data nodes that match the current query node based on number of neighbors and labels.
		Set<Long> firstPrune = new HashSet<>();

		//get label
		String label = subgraphQuery.get(qid).getLabel();
		//get number of neighbors
		int size = subgraphQuery.get(qid).getNeighbors().size();
		String cypher = "MATCH (u:`"+target +"`:"+label + ") \n" +
				"WITH u, size((u)--()) as degree \n" +
				"WHERE degree >= $size \n" +
				"RETURN id(u) as id";

		Map<String,Object> params = new HashMap<>();
		params.put( "size", size );

		Result result = tx.execute(cypher, params);
		while(result.hasNext()){
			Map<String, Object> map = result.next();
			Long id = (Long) map.get("id");
			firstPrune.add(id);
		}

		return firstPrune;
	}

	private static boolean prune(String[] queryNodeProfile, long did, Transaction tx) {
		// TODO Your code here!!!!
		// Get the profile of the node with id in the database and compare it with the query node profile.
		String[] dataNodeProfile = (String[]) tx.getNodeById(did).getProperty("profile");
//		System.out.println("-----------------------------------------------------");
//		for(String str: dataNodeProfile)
//			System.out.print(str + " ");
//		System.out.println();
//		System.out.println("-----------------------------------------------------");
		int i =0, j =0;
		while(i < queryNodeProfile.length && j < dataNodeProfile.length){
			while(j < dataNodeProfile.length && !queryNodeProfile[i].equals(dataNodeProfile[j]))
				j++;
			if(i < queryNodeProfile.length && j == dataNodeProfile.length)
				break;
			i++;
			j++;
		}
		if(i == queryNodeProfile.length)
			return false;
		return true;
	}
	
	private static void pruneGlobally(Map<Long, SubGraph> subgraphQuery, Transaction tx, Map<Long, Set<Long>> candidates) {
		// TODO Your code here!!!!
		// Global pruning of the candidates. You can use any library for computing a maximum cardinality bipartite matching
		//		or you can implement your own. If the latter, make sure your matching computation is correct before proceeding
		//		with the global pruning implementation.
	}

	private static Map<Long, Integer> calculateSize(Map<Long, Set<Long>> candidates){
		Map<Long, Integer> hm = new HashMap<>();
		for(Long u: candidates.keySet()){
			int size = candidates.get(u).size();
			hm.put(u, size);
		}
		return hm;
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

	private static int getNumberOFConnections(List<Long> neighbors, List<Long> order){ //getDegree
		int result =0;
		for(Long n: neighbors){
			if(order.contains(n))
				result++;
		}
		return result;
	}
	
	private static int getDegree(Map<Long, SubGraph> subgraphQuery, List<Long> order, Long u) {
		// TODO Your code here!!!!
		// Get the degree between u and order in the query.
		//replaced this function with my own // just above
		return 0;
	}
	
	private static void subgraphIsomorphism(Map<Long, SubGraph> subgraphQuery, Transaction tx, Map<Long, Set<Long>> candidates, List<Long> order,
			int i, Map<Long, Long> currentFunction, List<Map<Long, Long>> allFunctionsFound, boolean isInduced) {
		// TODO Compute subgraph isomorphism using backtracking.
		//  Note that the original node id stored in the database is int and should be cast to long.

		//tx : G
		//subgraphQuery : S ; Map<Long, SubGraph>
		//candidates: C ; Map<Long, Set<Long>>
		//order : O ; processing order ; List<Long>
		//i : current position of the order
		//currentFunction: Phi ; Map<Long, Long>
		//allFunctionsFound :; Big Phi List<Map<Long, Long>>

		if(currentFunction.size() == subgraphQuery.size()){
			Map<Long, Long> hm = new HashMap<>(currentFunction);//BigPhi := BigPhi U Phi
			allFunctionsFound.add(hm);
//			for(Long subU: currentFunction.keySet()){
//				hm = new HashMap<>();
//				Long graphV = currentFunction.get(subU);
//				hm.put(subU, graphV);
//				allFunctionsFound.add(hm);
////				hm = new HashMap<>();
//			}
		}else{
			Long u = order.get(i);
			//get all the candidates of current u
			Set<Long> allPossibleCandidates = candidates.get(u);
			for(Long v: allPossibleCandidates){
				if(!currentFunction.containsValue(v) && isValid(tx, subgraphQuery, currentFunction, u, v, isInduced)){
					currentFunction.put(u, v);
					subgraphIsomorphism(subgraphQuery, tx, candidates, order, i + 1, currentFunction, allFunctionsFound, isInduced);
					currentFunction.remove(u);
				}
			}
		}

	}

	private static boolean isValid(Transaction tx, Map<Long, SubGraph> subgraphQuery,
								   Map<Long, Long> currentFunction, Long u, Long v, boolean isInduced){
		boolean valid = true;
		//getting neighbors of current u
		List<Long> neighbors = subgraphQuery.get(u).getNeighbors();


		//code given by professor starts here
		if (neighbors != null) {
			for(Long uPrime: neighbors){
				if(currentFunction.containsKey(uPrime) && !ifConnectionBetweenNodes(v, currentFunction.get(uPrime), tx)){
					valid =  false;
				}
			}
			if(isInduced){
				for(Long uPrime : currentFunction.keySet()){
					if(!neighbors.contains(uPrime) && ifConnectionBetweenNodes(v, currentFunction.get(uPrime), tx)){
						valid = false;
					}
				}
			}
		}


		return valid;
	}

	private static boolean ifConnectionBetweenNodes(Long v, Long vPrime, Transaction tx){
//		String str = "MATCH (x) -- (y)\n WHERE id(x) = "+ v + "AND id(y) = "+ vPrime + "\n Return ";
//		int relationships = 0;
//		String str2 = "START id(x)=" + v + ", id(x)="+vPrime+"\n" +
//				".. MATCH x-[:r]->y\n" +
//				".. RETURN count(r) as "+relationships;
		String cypher = "MATCH (x) -- (y) \n " +
				"WHERE id(x) = $v AND id(y) = $vPrime\n" +
				"RETURN x.id as idX, y.id as idY";
		Map<String,Object> params = new HashMap<>();
		params.put("v", v);
		params.put("vPrime", vPrime);
		Result res = tx.execute(cypher, params);
		if(res.hasNext()){
			return true;
		}
		res.close();
//		if(relationships == 0){
//			return false;
//		}
		return false;
	}
	
}
