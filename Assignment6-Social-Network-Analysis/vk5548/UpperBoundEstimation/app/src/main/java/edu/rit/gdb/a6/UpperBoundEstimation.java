package edu.rit.gdb.a6;

import java.io.File;
import java.util.*;

import org.apache.commons.collections.map.HashedMap;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class UpperBoundEstimation {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final int ki = Integer.valueOf(args[1]), ke = Integer.valueOf(args[2]);

		System.out.println(new Date() + " -- Started");
		System.out.println("neo4jFolder : "+ neo4jFolder);
		System.out.println("ki : "+ ki);
		System.out.println("ke : "+ ke);
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");


		//---------------
		// Set psiest=degree.
		int step = 5000;
		Transaction tx = db.beginTx();
		long cnt = (long) tx.execute("MATCH (n) RETURN count(n) AS cnt").next().get("cnt");
		tx.close();
		long copycnt = cnt;

		int times = 0;
		while (cnt > 0) {
			tx = db.beginTx();
			cnt -= (long) tx.execute("MATCH (n) WHERE id(n) >= " + (times*step) + " AND id(n) <= " + ((times+1)*step) +
					" SET n = {} RETURN COUNT(n) AS cnt").next().get("cnt");
			tx.commit();
			tx.close();
			times++;
		}

		cnt = copycnt;
		times = 0;
		while (cnt > 0) {
			tx = db.beginTx();
			cnt -= (long) tx.execute("MATCH (n) WHERE id(n) >= " + (times*step) + " AND id(n) <= " + ((times+1)*step) +
					" WITH n, size((n)--()) AS degree SET n.psiest = degree RETURN count(n) AS cnt").next().get("cnt");
			tx.commit();
			tx.close();
			times++;
		}


		//---------------
		
		// TODO: Your code here!!!!
		// Each node in the input database will have its 'psiest' property set. You should focus only on the nodes with psiest between ki and ke.
		//		If a certain node is refined, you must update its 'psiest' property.

		String cypherWindowNodes = "MATCH (u) \n" +
				"WHERE u.psiest >= $ki AND u.psiest <= $ke  \n"  +
				"RETURN id(u) as id, u.psiest as psiest, size((u) --()) \n" +
				"ORDER BY u.psiest, id(u)";
		Map<String, Object> cypherWindowNodesParams = Map.of("ki", ki, "ke", ke);
		Transaction txWindowNodes = db.beginTx();
		Result resWindowNodes  = txWindowNodes.execute(cypherWindowNodes, cypherWindowNodesParams);
//		Set<Long> zSet = new HashSet<>();
		Map<Long, Long> zSet = new HashMap<>();
		Transaction txGetNeighbors = null, txCountNeighbors = null, txSetPsiest = null;
		Long function = null;
		while(resWindowNodes.hasNext()){
			Map<String, Object> resultMap = resWindowNodes.next();
			Long nodeID = (Long) resultMap.get("id");
			Long nodePsiest = (Long) resultMap.get("psiest");
			//getting the zSet
			String findNeighbors = "MATCH (u) -- (x) \n" +
					"WHERE id(u) = $nodeID and x.psiest < $psiest \n" +
					"RETURN id(x) as neighborID, x.psiest as neighborsPsiest \n" +
					"ORDER BY x.psiest";
			txGetNeighbors = db.beginTx();
			Result resNeighbors = txGetNeighbors.execute(findNeighbors, Map.of("nodeID", nodeID, "psiest", nodePsiest));
			while(resNeighbors.hasNext()){
				Map<String, Object> zMap = resNeighbors.next();
				Long currentNeighorID =(Long)zMap.get("neighborID");
				Long currentNeighorPsiest =(Long)zMap.get("neighborsPsiest");
				//need the list too
				zSet.put(currentNeighorID, currentNeighorPsiest);
			}
			txGetNeighbors.close();
			resNeighbors.close();
			//getting the count of neighbors
			String countAllNeighbors = "MATCH (u) -- (x) \n" +
					"WHERE id(u) = $nodeID \n" +
					"RETURN COUNT(x) as allNeighborsCount";
			txCountNeighbors = db.beginTx();
			Long totalNeighbors = (Long) txCountNeighbors.execute(countAllNeighbors, Map.of("nodeID", nodeID)).next().get("allNeighborsCount");
			txCountNeighbors.close();
			//checking the if condition
			if(totalNeighbors - zSet.size() < nodePsiest){
				//zSet is already sorted
				int i = 0;
				for(Long key: zSet.keySet()){
					Long currentPsiest = zSet.get(key);
					function = Math.max(totalNeighbors - (i + 1), currentPsiest);
					if(function < nodePsiest){

						System.out.print("nodeID :" + nodeID);
						System.out.println(" function :" + function);
						String setPsiest = "MATCH (u)\n" +
								"WHERE id(u) = $nodeID\n" +
								"SET u.psiest = $function";
						txSetPsiest = db.beginTx();
						txSetPsiest.execute(setPsiest, Map.of("nodeID", nodeID , "function", function));
						txSetPsiest.commit();
						txSetPsiest.close();

					}
					i++;
				}
			}
		}
		txWindowNodes.close();
		resWindowNodes.close();
		// TODO: End of your code.
		
		service.shutdown();

		System.out.println(new Date() + " -- Done");
	}

}
