package edu.rit.gdb.a5;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class AdaptiveProcessingOrder {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final String dagStr = args[1];
		final boolean induced = Boolean.valueOf(args[2]);
		final String resultsFile = args[3];
		
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		
		List<Map.Entry<Long, Long>> dag = Arrays.stream(dagStr.substring(0, dagStr.length()-1).split(",")).
				map(e->Map.entry(Long.valueOf(e.substring(0, e.indexOf(">"))), Long.valueOf(e.substring(e.indexOf(">")+1, e.length())))).collect(Collectors.toList());
		
		Transaction tx = db.beginTx();
		System.out.println("neo4jFolder : " + neo4jFolder);
		System.out.println("dagStr : " + dagStr);
		System.out.println("induced : " + induced);
		System.out.println("resultsFile : " + resultsFile);
		System.out.println("dag : " + dag);
		// Use this variable to build your query.
		Map<Long, Set<Long>> subgraphQuery = new HashMap<>();
		try {

			for(Map.Entry<Long, Long> single: dag){
				Long key = single.getKey();
				Long val = single.getValue();
				if(val != -1){
					Set<Long> s = new HashSet<>();
					//check if map contains the key as key
					if(!subgraphQuery.containsKey(key)){ // if no, add key-val
						s = new HashSet<>();
					}else{ //else get val and then update the val
						s = subgraphQuery.get(key);
					}
					s.add(val);
					subgraphQuery.put(key, s);
					//check if map contains the val as key
					if(!subgraphQuery.containsKey(val)){
						s = new HashSet<>();

					}else{
						s = subgraphQuery.get(val);
					}
					s.add(key);
					subgraphQuery.put(val, s);
				}else{
					Set<Long> s = new HashSet<>();
					if(!subgraphQuery.containsKey(key)){ // if no, add key-val
						s = new HashSet<>();
						s.add(val);
						subgraphQuery.put(key, s);
					}
				}

			}

				// TODO Your code here!!!

		} catch (Throwable oops) {
			// Awful issue!
			oops.printStackTrace();
		}
		
		if (subgraphQuery != null) {
			StringBuffer progress = new StringBuffer();
			
			// Subgraph matching.
			Map<Long, Set<Long>> parents = getParents(dag);
			System.out.println("parents :  " + parents);
			Map<Long, Set<Long>> children = getChildren(dag);
			System.out.println("children :  " + children);
			subgraphIsomorphism(subgraphQuery, parents, children, tx, 0, new HashMap<>(), new ArrayList<>(), induced, progress);
			
			PrintWriter writer = new PrintWriter(new File(resultsFile));
			writer.println(progress);
			writer.close();
		}
		
		tx.close();
		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}
	
	private static Map<Long, Set<Long>> getParents(List<Map.Entry<Long, Long>> dag) {
		Map<Long, Set<Long>> parents = new HashMap<>();
		
		// TODO Your code here!!!
		for(Map.Entry<Long, Long> single: dag){
			Long key = single.getKey();
			Long val = single.getValue();
			Set<Long> s = new HashSet<>();
			if(parents.containsKey(key)){
				s = parents.get(key);
			}
				s.add(val);
				parents.put(key, s);

		}
		// Get the parents using DAG.
		
		return parents;
	}

	private static Map<Long, Set<Long>> getChildren(List<Map.Entry<Long, Long>> dag) {
		Map<Long, Set<Long>> children = new HashMap<>();
		for(Map.Entry<Long, Long> dg : dag){
			Long val = dg.getKey();
			Long key = dg.getValue();
			Set<Long> s = new HashSet<>();
			if(key != -1){
				if(children.containsKey(key)){
					s = children.get(key);
				}
				s.add(val);
				children.put(key,s);
			}

		}
		return children;
	}
	
	private static void subgraphIsomorphism(Map<Long, Set<Long>> subgraphQuery, Map<Long, Set<Long>> parents, Map<Long, Set<Long>> children, Transaction tx, int i, Map<Long, Long> phi,
			List<Map<Long, Long>> Phi, boolean isInduced, StringBuffer progress) {
//		dag : [1=-1, 6=0, 0=1, 2=1, 4=1, 3=2, 5=4, 7=6]
		if (i == parents.size()) {
			Phi.add(getOriginalIds(tx, phi));
			
			// Record solution found.
			if (progress.length() != 0)
				progress.append(",");
			progress.append("Solution found!");
		} else {
			Long next = null;
			Set<Long> CM = new HashSet<>();
			
			// Select next node to process.
			// If first node in the order, get the root.
			if (i == 0) {
				// TODO Your code here!
				// Using the DAG, determine the children of the root. CM for the root is the union of all source nodes
				//	such that the edge is annotated with 'root-child'.
				for(Long root: parents.keySet()){
					Set<Long> parent = parents.get(root);
					if(parent.size() != 1)
						continue;
					if(parent.contains(Long.valueOf(-1))){
						next = root;
					}
				}

				//get CM for root such that edge is annotated as root-child
				//get children of the root
				Set<Long> rootChildren = children.get(next);
				String relationship = "";
				for(Long child: rootChildren){
					relationship+= next + "-"+child;
					String cypher = "MATCH u -[:"+relationship+"]- v \n" +
							"RETURN id(u) as u id(v) as v";
					Result res = tx.execute(cypher);
					while(res.hasNext()){
						Map<String, Object> mp = res.next();
						for(Map.Entry<String, Object> obj: mp.entrySet()){

							Long val = Long.valueOf((String) obj.getValue());
							CM.add(val);

						}
					}
				}

			} else {
				// TODO Your code here!
				// Not the root. First, determine the query nodes that are extendable, that is, all its parents are 
				//	present in the solution.
				boolean flag = true;
				for(Long child: parents.keySet()){
					Set<Long> par = parents.get(child);

					for(Long p: par){
						if(!phi.containsKey(p)){
							flag = false;
						}
					}
					if(flag){
						next = child;
					}
					//get children of the root
					Set<Long> rootChildren = children.get(next);
					String relationship = "";
					for(Long c: rootChildren){
						relationship+= next + "-"+c;
						String cypher = "MATCH u -[:"+relationship+"]- v \n" +
								"RETURN id(u) as u id(v) as v";
						Result res = tx.execute(cypher);
						while(res.hasNext()){
							Map<String, Object> mp = res.next();
							for(Map.Entry<String, Object> obj: mp.entrySet()){

								Long val = Long.valueOf((String) obj.getValue());
								CM.add(val);

							}
						}
					}
				}
				
				// Then, compute CM for each of them, that is, the candidates they can be mapped to.
				//	For each u, get the parents of u in the DAG. For each up, check the edges up-u attached to vp=\phi(up).
				//	Important! Do not remove ran phi at this stage!
				
				
				// Finally, select u as the node with minimum |CM|. If there are ties, select the u with minimum id.
				
			}
			
			
			
			// Record u and size of CM.
			if (progress.length() != 0)
				progress.append(",");
			progress.append("(");
			progress.append(i);
			progress.append(",");
			progress.append(next);
			progress.append(",");
			progress.append(CM.size());
			progress.append(")");
			
			List<Long> CMList = new ArrayList<>(CM);
			Collections.sort(CMList);
			for (Long v : CMList) {
				if (phi.containsValue(v))
					continue;
				
				// If it is non-induced, nothing extra must be done. If it is induced, then we need to check only induced.
				if (!isInduced || isValidInduced(next, v, tx, subgraphQuery, phi)) {
					phi.put(next, v);
					subgraphIsomorphism(subgraphQuery, parents, children, tx, i+1, phi, Phi, isInduced, progress);
					phi.remove(next);
				}
			}
		}
	}
	
	private static Map<Long, Long> getOriginalIds(Transaction tx, Map<Long, Long> currentFunction) {
		// TODO Your code here!!!
		// Get the original ids of the current function from the database.
		return new HashMap<>();
	}
	
	private static boolean isValidInduced(Long u, Long v, Transaction tx, Object subgraphQuery, Map<Long, Long> phi) {
		// TODO Your code here!!!
		// Check induced only! You should not take direction into account, that is, do not use the DAG here.
		return false;
	}

}
