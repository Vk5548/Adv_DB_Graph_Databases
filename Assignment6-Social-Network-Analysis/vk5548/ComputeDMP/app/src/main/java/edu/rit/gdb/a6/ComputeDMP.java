package edu.rit.gdb.a6;

import java.io.File;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class ComputeDMP {
	
	public static void main(String[] args) throws Exception {
		// TODO PROF CODE STARTS
		final String neo4jFolder = args[0];

		System.out.println(new Date() + " -- Started");
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");

		// Every node will have its psi property set.
		// Recall that you must compute realistic ranks, so you must keep track of all the nodes that have repeated values to provide their ranks.
		// You must set properties rankc and rankd for each node.
		// TODO PROF CODE ENDS


		// TODO RANK D STARTS HERE
		Transaction txSortD = db.beginTx();
		String sortD ="Match (u) \n" +
				"WITH u,  size((u)--()) as degree \n" +
				"RETURN id(u) as id, degree \n" +
				"ORDER BY degree DESC";
		Result resSortD = txSortD.execute(sortD);

		// Initialize variables
		int counter = 1;
		int previousDegree = -1;
		int sumOfGroupedRanks = 0;
		Set<Long> groupedRankIds = new HashSet<>();
		// Iterate over all nodes
		Transaction txSetrank = null;
		while(resSortD.hasNext()){
			Map<String, Object> map = resSortD.next();
			long nodeId = Long.parseLong(map.get("id").toString());
			int degree = Integer.parseInt(map.get("degree").toString());
			// If previous degree is NOT the same
			if (degree != previousDegree && sumOfGroupedRanks != 0) {
				// get realistic rank
				int avgRank = sumOfGroupedRanks / groupedRankIds.size();
				// Update all previously grouped ids with realistic rank
				for (long id : groupedRankIds) {
					String set= "MATCH (u) \n" +
							"WHERE id(u) = $nodeID \n" +
							"SET u.rankd = $avgrank";
					Map<String, Object> m = Map.of("nodeID", id,"avgrank", avgRank);
					txSetrank = db.beginTx();
					txSetrank.execute(set, m);
					txSetrank.commit();
					txSetrank.close();

				}
				// Empty the grouped rank ids set
				groupedRankIds = new HashSet<>();
				sumOfGroupedRanks = 0;
			}
			// Add the current id to the set
			groupedRankIds.add(nodeId);
			sumOfGroupedRanks += counter;
			// Increment the counters
			previousDegree = degree;
			counter++;
		}
		// For the last set of grouped ids

		// get realistic rank
		int avgRank = sumOfGroupedRanks / groupedRankIds.size();
		// Set realistic rank
		for (long id : groupedRankIds) {
			String set= "MATCH (u) \n" +
					"WHERE id(u) = $nodeID \n" +
					"SET u.rankd = $avgrank";
			Map<String, Object> m = Map.of("nodeID", id,"avgrank", avgRank);
			txSetrank = db.beginTx();
			txSetrank.execute(set, m);
			txSetrank.commit();
			txSetrank.close();
		}
		txSortD.close();
		resSortD.close();

		// TODO RANK D ENDS HERE



		// TODO RANK C STARTS HERE
		Transaction txSortC = db.beginTx();
		String sortC ="Match (u) \n" +
				"WITH u,  size((u)--()) as degree \n" +
				"RETURN id(u) as id, degree, u.psi as psi \n" +
				"ORDER BY psi, degree DESC";
		Result resSortC = txSortC.execute(sortC);

		// Initialize variables
		counter = 1;
		previousDegree = -1;
		int previousPsi = -1;
		sumOfGroupedRanks = 0;
		groupedRankIds = new HashSet<>();
		// Iterate over all nodes
		txSetrank = null;
		while(resSortD.hasNext()){
			Map<String, Object> map = resSortD.next();
			long nodeId = Long.parseLong(map.get("id").toString());
			int degree = Integer.parseInt(map.get("degree").toString());
			int psi = Integer.parseInt(map.get("psi").toString());
			// If previous degree is NOT the same
			if ((degree != previousDegree || psi != previousPsi) && sumOfGroupedRanks != 0 ) {
				// get realistic rank
				avgRank = sumOfGroupedRanks / groupedRankIds.size();
				// Update all previously grouped ids with realistic rank
				for (long id : groupedRankIds) {
					String set= "MATCH (u) \n" +
							"WHERE id(u) = $nodeID \n" +
							"SET u.rankd = $avgrank";
					Map<String, Object> m = Map.of("nodeID", id,"avgrank", avgRank);
					txSetrank = db.beginTx();
					txSetrank.execute(set, m);
					txSetrank.commit();
					txSetrank.close();

				}
				// Empty the grouped rank ids set
				groupedRankIds = new HashSet<>();
				sumOfGroupedRanks = 0;
			}
			// Add the current id to the set
			groupedRankIds.add(nodeId);
			sumOfGroupedRanks += counter;
			// Increment the counters
			previousPsi = psi;
			previousDegree = degree;
			counter++;
		}
		// For the last set of grouped ids

		// get realistic rank
		avgRank = sumOfGroupedRanks / groupedRankIds.size();
		// Set realistic rank
		for (long id : groupedRankIds) {
			String set= "MATCH (u) \n" +
					"WHERE id(u) = $nodeID \n" +
					"SET u.rankd = $avgrank";
			Map<String, Object> m = Map.of("nodeID", id,"avgrank", avgRank);
			txSetrank = db.beginTx();
			txSetrank.execute(set, m);
			txSetrank.commit();
			txSetrank.close();
		}
		txSortD.close();
		resSortD.close();

		// TODO RANK C ENDS HERE



		//TODO PROF CODE
		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}

}
