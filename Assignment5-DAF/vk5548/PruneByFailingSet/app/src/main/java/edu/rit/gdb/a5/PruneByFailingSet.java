package edu.rit.gdb.a5;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

public class PruneByFailingSet_Template {

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
		
		// Use this variable to build your query.
		Object subgraphQuery = null;
		
		try {
			// TODO Your code here!!!
			
		} catch (Throwable oops) {
			// Awful issue!
			oops.printStackTrace();
		}
		
		if (subgraphQuery != null) {
			// The processing order will be based on the DAG.
			List<Long> order = new ArrayList<>();
			// TODO Add root first. Then, add u to the order only if up is in the order.
			
			// Subgraph matching.
			Map<Long, Set<Long>> parents = getParents(dag), ancestors = getAncestors(dag);
			
			// Initialize F.
			// For each query node in the order, we will create a list of failing sets to record information of the backtracking process.
			Map<Integer, List<Set<Long>>> F = new HashMap<>();
			for (int i = 0; i <= order.size(); i++)
				F.put(i, new ArrayList<>());
			
			StringBuffer progress = new StringBuffer();
			
			subgraphIsomorphism(subgraphQuery, parents, ancestors, tx, 0, order, new HashMap<>(), new ArrayList<>(), F, induced, progress);
			
			PrintWriter writer = new PrintWriter(new File(resultsFile));
			writer.println(progress);
			writer.close();
		}
		
		tx.close();
		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}
	
	private static Long getRoot(List<Map.Entry<Long, Long>> dag) {
		// TODO Return root!
//		return -1l;
		
		Long root = null;
		for (Map.Entry<Long, Long> entry : dag) {
			long u = entry.getKey(), up = entry.getValue();
			
			if (up == -1) {
				root = u;
				break;
			}
		}
		
		return root;
	}
	
	private static Set<Long> getQueryNodes(Object subgraphQuery) {
		// TODO Your code here!!!!
		// Get all the query nodes.
//		return new HashSet<>();
		
		return ((DAGSubgraphQuery) subgraphQuery).undirectedNeighbors.keySet();
	}
	
	// TODO Remove!!!!
	class DAGSubgraphQuery {
		Long root;
		Map<Long, Set<Long>> nodeNeighbors;
		Map<Long, Set<Long>> undirectedNeighbors;
	}
	
	private static Map<Long, Set<Long>> getParents(List<Map.Entry<Long, Long>> dag) {
		Map<Long, Set<Long>> parents = new HashMap<>();
		
		// TODO Your code here!!!
		// Get the parents using DAG.
		
		return parents;
	}
	
	private static Map<Long, Set<Long>> getAncestors(List<Map.Entry<Long, Long>> dag) {
		Map<Long, Set<Long>> ancestors = new HashMap<>();
		
		// TODO You code here!
		// Get the ancestors using DAG. Recall that the ancestors of u also contain u.
		
		return ancestors;
	}
	
	private static void subgraphIsomorphism(Object subgraphQuery, Map<Long, Set<Long>> parents, Map<Long, Set<Long>> ancestors, 
			Transaction tx, int i, List<Long> order, Map<Long, Long> phi,  List<Map<Long, Long>> Phi, Map<Integer, List<Set<Long>>> F, 
			boolean isInduced, StringBuffer progress) {
		if (i == parents.size()) {
			Phi.add(getOriginalIds(tx, phi));
			
			// TODO Solution class.
			
			progress.append("Solution class, failing set: " + F.get(i) + "; ");
		} else {
			Long next = order.get(i);
			Set<Long> CM = new HashSet<>();
			
			// If first node, get the root.
			if (i == 0) {
				// TODO Using the DAG, determine the children of the root. CM for the root is the union of all source nodes
				//	such that the edge is annotated with 'root-child'.
			} else {
				// TODO Not the root. Compute CM, that is, the candidates next can be mapped to.
				//	Get the parents of next in the DAG. For each up, check the edges up-next attached to vp=\phi(up).
				//	Important! Do not remove ran phi at this stage!
			}
			
			if (CM.isEmpty()) {
				// TODO Empty-set class.
				// No candidates for next. The culprits are the ancestors of next.
				
				progress.append("Empty-set class, failing set: " + F.get(i) + "; ");
			} else {
				F.get(i).clear();
				
				for (Long v : CM) {
					if (phi.containsValue(v)) {
						// TODO Conflict class.
						
						progress.append("Conflict class, failing set: " + F.get(i) + "; ");
						
						continue;
					}
					
					// If it is non-induced, nothing extra must be done. If it is induced, then we need to check only induced.
					Long nextNext = null;
					if (!isInduced || isValidInduced(next, v, tx, subgraphQuery, phi)) {
						phi.put(next, v);
						subgraphIsomorphism(subgraphQuery, parents, ancestors, tx, i+1, order, phi, Phi, F, isInduced, progress);
						phi.remove(next);
						
						List<Set<Long>> subtree = F.get(i+1);
						boolean emptySetFound = false;
						Set<Long> failingSetWithoutUn = null, union = new HashSet<>();
						
						// TODO Your code here!
						// Get the failing sets collected in the subtree and make decisions based on that.
						//		If there is an empty failing set: nothing can be done, i.e., at least a full or partial solution was found. 
						//			Add partial/full (empty failing set) as current.
						//		Else If there is a failing set that does not contain the query node in the order.
						//			Add failing set as current.
						//		Otherwise, add the union of all failing sets as current.
						
						// Process what we found.
						Set<Long> current = null;
						if (/* TODO Case 1 */ ) {
							progress.append("Case 1, failing set: " + current + "; ");
						} else if (/* TODO Case 2 */) {
							progress.append("Case 2, failing set: " + current + "; ");
						} else {
							/* TODO Case 3 */
							progress.append("Case 3, failing set: " + current + "; ");
						}
						
						// Add new failing set.
						F.get(i).add(current);
						
						// Clear the subtree.
						subtree.clear();
						
						if (/* TODO Backjump? */ ) {
							progress.append("Backjump; ");
							
							// We can stop this search!
							break;
						}
					} else {
						// TODO Partial, non-induced solution.
						progress.append("Solution class (partial), failing set: " + F.get(i) + "; ");
					}
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
