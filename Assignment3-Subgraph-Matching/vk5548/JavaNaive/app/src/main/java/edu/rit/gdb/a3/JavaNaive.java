package edu.rit.gdb.a3;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;

public class JavaNaive {

//	public class SubGraph{
//		String nodeU;
//		String label;
//		List<String> neighbors;
//
//		SubGraph(String nodeU, String label, List<String> neighbors){
//			this.nodeU = nodeU;
//			this.label = label;
//			this.neighbors = neighbors;
//
//		}
//	}
//
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0],
				proteinsFolder = args[1],
				listOfTargets = args[2],
				listOfQueries = args[3];
		final boolean induced = Boolean.valueOf(args[4]);
		final String resultsFolder = args[5];
		
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
				String query = new String(Files.readAllBytes(Paths.get(allQueryFiles.get(selectedQuery).toURI())));
				List<Map<Long, Long>> results = new ArrayList<>();
				try {
					results = matching(tx, query, target, induced);
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
	
	public static List<Map<Long, Long>> matching(Transaction tx, String query, String targetDb, boolean isInduced) throws IOException {
		List<Map<Long, Long>> result = new ArrayList<>();
		
		// TODO Your code here!!!
		// Use this variable to build your query.
		Map<Map<Long, String>, List<Long>> subgraphQuery = new HashMap<>();
//		JavaNaive jn = new JavaNaive();
//		JavaNaive.SubGraph subgraphQuery ;
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
			labels.put(Long.valueOf(nodeWithLabels[0]), nodeWithLabels[1]);
		}
		//processing neighbors:
		Map<Long, List<Long>> neighbors = new HashMap<>();
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
		//BUILDING THE SUBGRAPH QUERY
		for (Map.Entry<Long,String> entry : labels.entrySet()){
			Long key = entry.getKey();
			String label = entry.getValue();
			Map<Long, String> inner = new HashMap<>();
			inner.put(key, label);
//			subgraphQuery = jn.new SubGraph(key, label, neighbors.get(key));
			subgraphQuery.put(inner, neighbors.get(key)); //getting the list of neighbors
		}

		//releasing memory of labels and neighbors hashmap
		labels = new HashMap<>();
		neighbors = new HashMap<>();

			// Perform subgraph isomorphism.
		// The query must return the original id (not the internal, the property id) of each identified node.
		// Recall that to compute induced subgraph matching you must check that, if there is no edge between
		//		two query nodes, there must be no edge between the mapped data nodes.
		// The subgraphQuery object will contain a graph structure to store the query at hand. Generate this
		//		query from the file once and store it in memory. Recall that you must deal with labels and
		//		neighbors, so your graph structure should support this. You can use your own in-memory data
		//		structure or use any external library for this.

		//build the query:

		
		// TODO End of your code.
		
		Map<Long, Set<Long>> candidates = computeCandidates(subgraphQuery, tx, targetDb);
		System.out.println("Executed : candidates\n" + candidates.size());
		
		List<Long> order = computeProcessingOrder(subgraphQuery, tx, candidates);
		System.out.println("Executed : computeProcessingOrder\n" + order.toString());
		subgraphIsomorphism(subgraphQuery, tx, candidates, order, 0, new HashMap<>(), result, isInduced);
		System.out.println("Executed : subgraphIsomorphism"+ result.size());
		for(Map<Long, Long> mapping: result){
			for(Long u: mapping.keySet()){ //runs 1 time
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
					mapping.put(u, x);
				}
				result1.close();
				System.out.println(map.toString());
//				Long x = Long.valueOf("" + tx.getNodeById(mapping.get(v)).getProperty("id")) ;
//				if(x != null)
//					mapping.put(u, x);
			}
		}
		return result;
	}
	private static Set<Long> computeSingleNodeCandidates(String targetDb, int size, Transaction tx, String subLabel){
		Set<Long> result = new HashSet<>();
		ResourceIterator<Node> res = tx.findNodes(Label.label(targetDb));
		int degree;
		while(res.hasNext()){
			Node x =res.next();
			Iterable<Label> allLabels = x.getLabels();
			Iterator<Label> all = allLabels.iterator();
			boolean flag = false;
			while(all.hasNext()){ //runs time
				Label l = all.next();
				if(l.toString().equals(subLabel)){
					flag = true;
					break;
				}
			}
			if(flag){
				degree = x.getDegree(RelationshipType.withName(targetDb));
				if(degree >= size){
					result.add(x.getId());
				}
//				flag = false;
			}


		}
		res.close();
		return result;
	}
	
	private static Map<Long, Set<Long>> computeCandidates(Map<Map<Long, String>, List<Long>> subgraphQuery, Transaction tx, String targetDb) {
		Map<Long, Set<Long>> candidates = new HashMap<>();
		// TODO Compute candidates, that is, nodes in the database that can be mapped to each node in the query.
		String label = "";
		Set<Long> resultSet;
		for(Map<Long, String> outerKey : subgraphQuery.keySet()){//for each node
			for(Long innerKey: outerKey.keySet()){  //will execute only 1 time
				resultSet = new HashSet<>();
				label = outerKey.get(innerKey);
				Integer size = subgraphQuery.get(outerKey).size();

//				Set<Long> singleNodeCandidates= computeSingleNodeCandidates(targetDb, size, tx, label);
//				candidates.put(Long.valueOf(innerKey), singleNodeCandidates);
				String cypher = "MATCH (u:`"+targetDb +"`:"+label + ") \n" +
						"WITH u, size((u)--()) as degree \n" +
						"WHERE degree >= $size \n" +
						"RETURN id(u) as id";

				Map<String,Object> params = new HashMap<>();
				params.put( "size", size );

				Result result = tx.execute(cypher, params);
				while(result.hasNext()){
					Map<String, Object> map = result.next();
						Long id = (Long) map.get("id");
						resultSet.add(id);

					int i=0;
				}
				candidates.put(Long.valueOf(innerKey), resultSet);
			}
		}
		return candidates;
	}

	private static List<Long> computeProcessingOrder(Map<Map<Long, String>, List<Long>> subgraphQuery, Transaction tx, Map<Long, Set<Long>> candidates) {
		List<Long> order = new ArrayList<>();
		// TODO Compute the processing order of the query nodes. The order should contain all query nodes.
		//			The processing order should preserve neighborhood connectivity.
		Set<Long> nodes = new HashSet<>();
		for(Map<Long, String> outer: subgraphQuery.keySet()){
			List<Long> neigh = subgraphQuery.get(outer);
			for(Long inner: outer.keySet()) { //runs only 1 time
				if(order.isEmpty()){
					nodes.add(inner);
//					for(Long n: neigh){
//						nodes.add(n);
//					}
				}else{
					if(nodes.contains(inner)){
						for(Long n: neigh){
							nodes.add(n);
						}
					}else{
						for(Long n: neigh){
							if(nodes.contains(n)){
								nodes.add(inner);
								nodes.addAll(neigh);
								break;
							}
						}
					}

				}
			}

		}
		if(!(nodes.size() == subgraphQuery.size())){
			for(Map<Long, String> outer: subgraphQuery.keySet()){
				for(Long inner: outer.keySet()) {
					if(!nodes.contains(inner))
						nodes.add(inner);
				}//runs only 1 time
			}


		}
		for(Long n: nodes){
			order.add(n);
		}

		return order;
	}
	
	private static void subgraphIsomorphism(Map<Map<Long, String>, List<Long>> subgraphQuery, Transaction tx, Map<Long, Set<Long>> candidates, List<Long> order,
			int i, Map<Long, Long> currentFunction, List<Map<Long, Long>> allFunctionsFound, boolean isInduced) {
		//tx : G
		//subgraphQuery : S ; Map<Map<String, String>, List<String>>
		//candidates: C ; Map<Long, Set<Long>>
		//order : O ; processing order ; List<Long>
		//i : current position of the order
		//currentFunction: Phi ; Map<Long, Long>
		//allFunctionsFound :; Big Phi List<Map<Long, Long>>

		// TODO Compute subgraph isomorphism using backtracking.
		//  Note that the original node id stored in the database is int and should be cast to long.


		if(currentFunction.size() == subgraphQuery.size()){
			Map<Long, Long> hm;//BigPhi := BigPhi U Phi
			for(Long subU: currentFunction.keySet()){
				hm = new HashMap<>();
				Long graphV = currentFunction.get(subU);
				hm.put(subU, graphV);
				allFunctionsFound.add(hm);
//				hm = new HashMap<>();
			}
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

	private static boolean isValid(Transaction tx, Map<Map<Long, String>, List<Long>> subgraphQuery,
								   Map<Long, Long> currentFunction, Long u, Long v, boolean isInduced){
		boolean valid = true;
		//getting neighbors of current u
		List<Long> neighbors = null;
		for(Map<Long, String> outerKey: subgraphQuery.keySet()){
			for(Long nodeU: outerKey.keySet()){
				Long node = Long.valueOf(nodeU);
				if(node == u){
					neighbors = subgraphQuery.get(outerKey);
				}
			}
		} //outerKey is u
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
