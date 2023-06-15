package edu.rit.gdb.a6;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;

public class BottomUpKCore {

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

	public static void main(String[] args) throws Exception {
		// The neo4jFolderCopy folder is just an exact copy of the database in the neo4jFolder folder. 
		final String neo4jFolder = args[0],
				neo4jFolderCopy = args[1];
		System.out.println("neo4jFolder : "+ neo4jFolder);
		System.out.println("neo4jFolderCopy : "+ neo4jFolderCopy);
		System.out.println(new Date() + " -- Started");
		
		BatchInserter dbOrig = BatchInserters.inserter(DatabaseLayout.of(
				Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, new File(neo4jFolder).toPath()).build()));
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolderCopy).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService dbCopy = service.database("neo4j");
		Transaction txDelete = dbCopy.beginTx();
		Transaction txCount = dbCopy.beginTx();
		Transaction txGet = dbCopy.beginTx();




//		String cypherCount = "Match (n) \n" +
//				"Return Count(n) as cnt";
//		Result res = txCopy.execute(cypherCount);
//
//		while(res.hasNext()){
//			Map<String, Object> map = res.next();
//			for(String str: map.keySet()){ // runs once
//				total =  Integer.parseInt(map.get(str).toString());
//			}
//		}
		//get the number of nodes
		int total =countNumberOfNodes(txCount);
		System.out.println("total " + total);
		int current = 0, totalBefore = 0;
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
						"WHERE degree < $curr \n" +
						"DETACH DELETE u";
				Map<String, Object> attr = new HashMap<>();
				attr.put("curr", current);
				txDelete.execute(delete, attr);
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
					"RETURN id(u) as id";
			Result allNodeIDs = txGet.execute(getAllNodeIDs);
			while(allNodeIDs.hasNext()){
				Map<String, Object> allNodeIdsMap = allNodeIDs.next();
				for(String str: allNodeIdsMap.keySet()){ // runs once
					Long nodeId = Long.valueOf(allNodeIdsMap.get(str).toString());
//					Map<String, Object> properties = dbOrig.getNodeProperties(nodeId);
//					properties.put("psi", current);
					dbOrig.setNodeProperty(nodeId, "psi", current);

				}
			}
			current += 1;
		}

		txCount.close();
		txGet.close();
		// TODO: Your code here!!!!
		// Both databases will contain the same graph at the beginning and you are supposed to keep removing nodes from the copy while
		//		updating the original database. Since we are just going to update the original database, BatchInserter will do the job
		//		much faster. You can use "MATCH (v) WITH v, size((v)--()) as degree" to compute the degree of the nodes in Cypher, you
		//		can use DETACH DELETE to remove nodes and their edges. You must fill the "psi" property in the original database.
		
		
		
		// TODO: End of your code.

		service.shutdown();
		dbOrig.shutdown();

		System.out.println(new Date() + " -- Done");
	}

}


//while (rs.hasNext()) {
//					Map<String, Object> nodeToBeDeleted = rs.next();
//					Long nodeID = null;
//					int deletedNodesCtr = 0;
//					for (String str : nodeToBeDeleted.keySet()) { // runs once
//						nodeID = Long.parseLong(nodeToBeDeleted.get(str).toString());
//						String deleteNode = "MATCH (n)\n" +
//								"WHERE id(n) = $nodeID\n" +
//								"DELETE n";
//						Map<String, Object> params = new HashMap<>();
//						params.put("nodeID", nodeID);
//						txCopy.execute(deleteNode, params);
//						deletedNodesCtr++;
//					}
//					total -= deletedNodesCtr;
//					if (totalBefore == total) {
//						break;
//					}
//				}