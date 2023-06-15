package edu.rit.gdb.a6;

import java.io.File;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.io.layout.DatabaseLayout;


public class TopDownKCore {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0],
				neo4jFolderCopy = args[1];
		final int kmin = Integer.valueOf(args[2]), 
				kmax = Integer.valueOf(args[3]),
				step = Integer.valueOf(args[4]);

		System.out.println(new Date() + " -- Started");

		System.out.println("neo4jFolder : "+ neo4jFolder);
		System.out.println("neo4jFolderCopy : "+ neo4jFolderCopy);
		System.out.println("kmin : "+ kmin);
		System.out.println("kmax : "+ kmax);
		System.out.println("step : "+ step);
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");

//		// TODO : CODE TO BE REMOVED :- STARTS HERE !!!
//		// TODO : lines 1 through 4
//		//Creating transaction for updating databases
		Transaction txGrading = null;
//		//initializing the transaction
		txGrading = db.beginTx();
//		//get the total number of nodes in the database;
		long cnt = (long) txGrading.execute("MATCH (n) RETURN count(n) AS cnt").next().get("cnt");
//   		txGrading.close();
		long copycnt = cnt;
		System.out.println("nodes in roig db : "+ cnt);
//		// Reset all properties.
		int times = 0;
		while (cnt > 0) {
			Map<String, Object> params = Map.of("init", (times*step), "end", (times+1)*step);

			txGrading = db.beginTx();
			cnt -= (long) txGrading.execute("MATCH (n) WHERE id(n) >= $init AND id(n) <= $end "
					+ "SET n = {} RETURN COUNT(n) AS cnt", params).next().get("cnt");
			txGrading.commit();
			txGrading.close();

			times++;
		}
		System.out.println("Properties are reset");
//		// Set psi, psiest and deposit. // line 1 through 4
		cnt = copycnt;
		times = 0;
		while (cnt > 0) {
			Map<String, Object> params = Map.of("init", (times*step), "end", (times+1)*step);

			txGrading = db.beginTx();
			cnt -= (long) txGrading.execute("MATCH (n) WHERE id(n) >= $init AND id(n) <= $end "
					+ "WITH n, size((n)--()) AS degree "
					+ "SET n.psi = null, n.psiest = degree, n.deposit = 0 RETURN count(n) AS cnt", params).next().get("cnt");
			txGrading.commit();
			txGrading.close();

			times++;
		}
		System.out.println("Properties are set : psi, psiest, deposit");
		// TODO : CODE TO BE REMOVED :- Ends HERE !!!

		// TODO: Your code here!!!!
		// At the beginning, each node will have psi=null, psiest=degree and deposit=0.
		// In each step, you must start with the nodes whose psiest is between ki and ke,
		//	where ke is the max degree at the beginning. You must update both ke and ki in each step.
		//	The upper bound refinement is generally the bottleneck of the algorithm; limit the upper bound
		//	refinement to no more than 10,000 nodes each time.
		//	The auxiliary folder must be removed in every step and, if there are enough nodes, use it to copy the
		//	relevant subgraph and compute the core numbers in a bottom-up fashion. Note that you must copy
		//	the deposits of the nodes as well.
		//	When you set the psi property of a node, you should remove its psiest property.

		// TODO :
		// TODO: Your code here!!!!
		// TODO: Get initial ke and compute ki.
		//
		//TODO: line 5 and 6
		// Get max degree.

		txGrading = db.beginTx();
		int maxPSIest = ((Number) txGrading.execute("MATCH (n) RETURN MAX(n.psiest) AS max").next().get("max")).intValue();
		System.out.println("maxPSIest :"+ maxPSIest);
		txGrading.close();

		int ke = maxPSIest, ki = Math.max(kmin, ke - step);
		// TODO: End of your code!!!!
		int iteration = 0;
		while (ke - ki > 0) {
			System.out.println("ke  " + ke);
			System.out.println("ki  " + ki);
			System.out.println("ke - ki : " + (ke - ki));
			System.out.println("iteration : " + iteration++);
			//line 8
			// Let's refine the upper bound estimations first.
//			upperBoundEstimation(db, ki, ke);

			//line 9
			List<Long> VPrime = new ArrayList<>(); // will contain list of all the nodes
			Map<Long, Set<Long>> allNeighbors = new HashMap<>(); // node and its neighbors
			Map<Long, Long> deposits = new HashMap<>(); // node and its corresponding deposits

			
			// TODO: Your code here!!!!
			// TODO: Get the nodes with no psi set and psiest is between ki and ke.


			Map<String, Object> windowParams = Map.of("ke", ke, "ki", ki);


			Long pmax = null, totalNodes = null;

			Transaction txGetMaxWindowDeposit = db.beginTx();
			String getPsiMaxWindow = "MATCH (n) \n" +
					"WHERE n.psiest >= $ki AND n.psiest <= $ke AND n.psi IS NULL \n" +
					"RETURN MAX(n.deposit) as max_deposit";
			// id (V), collect neighbors , deposit

			Long windowDepositMax = (Long) txGetMaxWindowDeposit.execute(getPsiMaxWindow, Map.of("ke", ke, "ki", ki)).next().get("max_deposit");
			txGetMaxWindowDeposit.close();
			//--------------------------
			Transaction txGetWindowNodes = db.beginTx();
			String cypherWindow = "MATCH (n) \n" +
					"WHERE n.psiest >= $ki AND n.psiest <= $ke AND n.psi IS NULL \n" +
					"RETURN  id(n) as id, n.deposit as deposit";
			Result resultWindow = txGetWindowNodes.execute(cypherWindow, Map.of("ke", ke, "ki", ki));
			int iter = 0;
			while(resultWindow.hasNext()){

				Map<String, Object> mp = resultWindow.next();

//				VPrime.add((Long) mp.get("id"));
				deposits.put((Long) mp.get("id"), (Long) mp.get("deposit"));

			}
			txGetWindowNodes.close();

			System.out.println("windowDepositMax : " + windowDepositMax);
			System.out.println("totalNodes : " + deposits.size());

			// TODO: End of your code!!!!
			

			if(deposits.size() != 0){
				if (/* TODO: If there are enough nodes. */deposits.size() + windowDepositMax >= ki) {
					System.out.println("if condition is true: Graph will be constructed");
					// Delete folder to start from scratch.
					FileUtils.deleteDirectory(new File(neo4jFolderCopy));

					BatchInserter inserter = BatchInserters.inserter(DatabaseLayout.of(
							Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, new File(neo4jFolderCopy).toPath()).build()));



					// TODO: Your code here!!!!

					// TODO: Copy the induced subgraph of VPrime: extract from db and copy into using inserter.
					Transaction txGetNeighbors = db.beginTx();
					String windowNodes = "MATCH (n) --> (x)\n" +
							"WHERE id(n) = $id AND x.psiest >= $ki AND x.psiest <= $ke " +
							"RETURN id(x) as neighborID";
					// id (V), collect neighbors , deposit
					for(Long nodeID: deposits.keySet()){
						//create node in the copy database
						if(!VPrime.contains(nodeID)){
							Map<String, Object> attr = new HashMap<>();
							attr.put("deposit", deposits.get(nodeID));
							inserter.createNode(nodeID, attr);
							VPrime.add(nodeID);
						}

						// get all the neighbors of the current node
						txGetNeighbors = db.beginTx();
						Map<String, Object> windowNodeParams = Map.of("id", nodeID,"ke", ke, "ki", ki);
						Result allWindowNodes = txGetNeighbors.execute(windowNodes, windowNodeParams);
						Set<Long> currentNodeNeighbors = new HashSet<>();
						while(allWindowNodes.hasNext()){
							Long neighborID = (Long)  allWindowNodes.next().get("neighborID");

							if(deposits.containsKey(neighborID)){
								if(!VPrime.contains(neighborID)){
									Map<String, Object> attr = new HashMap<>();
									attr.put("deposit", deposits.get(nodeID));
									inserter.createNode(neighborID, attr);
									VPrime.add(neighborID);
								}
								if(nodeID < neighborID){
									currentNodeNeighbors.add(neighborID);
									inserter.createRelationship(nodeID, neighborID, RelationshipType.withName(""), new HashMap<>());
								}
							}
						}
						txGetNeighbors.close();
						allNeighbors.put(nodeID, currentNodeNeighbors);
					}




					// TODO: End of your code!!!!

					inserter.shutdown();

					// Compute bottom-up in the copy.
					DatabaseManagementService otherService = new DatabaseManagementServiceBuilder(new File(neo4jFolderCopy).toPath()).
							setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
							setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
					GraphDatabaseService dbCopy = otherService.database("neo4j");
					//call the bottomUpDecomposition
					bottomUpDecomposition(db, dbCopy, (long) VPrime.size(), ki);
					otherService.shutdown();
				}
			}

			
			// TODO: Your code here!!!!
			// TODO: Set deposits if psi is set; otherwise, update psiest to ki-1.
			Transaction tx = null;
			for(Long n: allNeighbors.keySet()){
				String ifPsiDefined = "MATCH (u) -- (x) \n" +
						"WHERE id(u) = $nodeID AND u.psi IS NOT NULL AND x.psi IS NULL \n" +
						"SET x.deposit = x.deposit + 1";
				Map<String, Object> ifPsiDefinedParams = Map.of("nodeID", n);
				tx = db.beginTx();
				tx.execute(ifPsiDefined, ifPsiDefinedParams);
				tx.commit();
				tx.close();
				String setEstimate = "MATCH (u) \n" +
						"WHERE id(u) = $nodeID AND u.psi IS NULL \n" +
						"SET u.psiest = $kIMinusOne";
				Map<String, Object> parameters = Map.of("nodeID", n, "kIMinusOne", ki - 1);
				tx = db.beginTx();
				tx.execute(setEstimate, parameters);
				tx.commit();
				tx.close();

			}
			// TODO: End of your code!!!!
			
			
			// TODO: Your code here!!!!
			// TODO: Update ke and ki.
			ke = ki - 1;
			ki = Math.max(kmin, ke - step);
			// TODO: End of your code!!!!
		}
		
		// TODO: End of your code.
		
		service.shutdown();

		System.out.println(new Date() + " -- Done");
	}
	
	private static void upperBoundEstimation(GraphDatabaseService db, int ki, int ke) {
		// TODO Refine the upper bounds here!
	}
	
	private static void bottomUpDecomposition(GraphDatabaseService dbOrig, GraphDatabaseService dbCopy, long totalNodes, int ki) {
		System.out.println("-------------------------------------------------------------------------------------");
		// TODO: Bottom-up decomposition. Do not forget to use the deposit!
		Transaction txDelete = dbCopy.beginTx();
		Transaction txCount = dbCopy.beginTx();
		Transaction txGet = dbCopy.beginTx();
//		Transaction dbOrigtx =dbOrig.beginTx();
		//get the number of nodes
		int total =countNumberOfNodes(txCount);
		System.out.println("total " + total);
		int current = ki, totalBefore = 0;
		int iter = 0;

		while(total > 0){
			System.out.println("iter :" + iter++);
			System.out.println("current :" + current);
			while(true) {
				txDelete = dbCopy.beginTx();
				totalBefore = total;
				// delete all nodes such that numNeighbors < current
				String delete = "Match (u) \n" +
						"WITH u,  size((u)--()) as degree \n" +
						"WHERE degree + u.deposit < $curr \n" +
						"DETACH DELETE u";
				Map<String, Object> attr = new HashMap<>();
				attr.put("curr", current);
				Result res = txDelete.execute(delete, attr);
//				res.getQueryStatistics()..getNo
				 txDelete.commit();
				txDelete.close();
//				//let n be the number of nodes deleted
				//count the number of nodes in the database
				int n = countNumberOfNodes(txCount); //current number of nodes in the database
				total = n;
				System.out.println("total after deletion " + total);
				System.out.println("totalbefore after deletion " + totalBefore);
				if(totalBefore == total){
					break;
				}

			}
			//get all the nodes from the copy database i.e nodeId and then update psi value in orig graph
			String getAllNodeIDs = "MATCH (u) \n" +
					"RETURN id(u) as id"; // getting remaining nodes
			Result allNodeIDs = txGet.execute(getAllNodeIDs);
			Transaction origTx = dbOrig.beginTx();
			while(allNodeIDs.hasNext()){
				origTx = dbOrig.beginTx();
				Map<String, Object> allNodeIdsMap = allNodeIDs.next();
				Long nodeId = Long.valueOf(allNodeIdsMap.get("id").toString());
				String setPsi = "MATCH (n) \n" +
						"WHERE id(n) = $nodeID \n" +
						"SET n.psi = $current"; //in list
				Map<String, Object> setParams = new HashMap<>();
				setParams.put("nodeID", nodeId);
				setParams.put("current", current);
				origTx.execute(setPsi, setParams);
				origTx.commit();
				origTx.close();
			}
			current += 1;
		}
		txCount.close();
		txGet.close();
	}

	private static int countNumberOfNodes(Transaction tx){
		int result = 0;
		String cypherCount = "Match (n) \n" +
				"Return Count(n) as cnt";
		Result res = tx.execute(cypherCount);

		while(res.hasNext()){
			Map<String, Object> map = res.next();
			for(String str: map.keySet()){ // runs once
				result =  Integer.parseInt(map.get(str).toString());
			}
		}
		return  result;
	}

}
