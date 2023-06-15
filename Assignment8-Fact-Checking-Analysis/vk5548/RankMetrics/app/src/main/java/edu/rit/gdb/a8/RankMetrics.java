package edu.rit.gdb.a8;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.*;

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

public class RankMetrics {
	
	private enum SPLIT {TRAIN, VALID, TEST};
	
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final SPLIT split = SPLIT.values()[Integer.valueOf(args[1].toUpperCase())]; // valid or test
		final int maxK = Integer.valueOf(args[2]);
		
		System.out.println(new Date() + " -- Started");
		
		File neo4jDB = new File(neo4jFolder);
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4jDB.toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		Transaction tx = db.beginTx();
		
		// TODO: Your code here!!!!
		// Get the relationship with the minimum id in the split provided as input and store there the following properties:
		//		+ mr: Mean Rank
		//		+ mrr: Mean Reciprocal Rank
		//		+ amr: Adjusted Mean Rank
		//		+ hatk (where k=1 until maxK provided as input): Hits at K.
		//
		//	You should use the values rank_s (String from BigDecimal), rank_o (String from BigDecimal), total_s (int), total_o (int).

		// Initialize variables
		int splitNum = split.ordinal();



		BigDecimal numR = new BigDecimal(0);
		BigDecimal mr = new BigDecimal(0);
		BigDecimal mrr = new BigDecimal(0);
		BigDecimal[] hitsAtK = new BigDecimal[maxK+1]; // hitsAtK will be at position k-1 in this array
		for(int i=0; i < hitsAtK.length; i++) {
			hitsAtK[i] = new BigDecimal(0);
		}
		BigDecimal amr = new BigDecimal(0);

		// Query to obtain all triples in this split
		String query = "MATCH (s)-[p]->(o) \n" +
				"WHERE p.split = $split \n" +
				"RETURN p.rank_s as rank_s, p.rank_o as rank_o, p.total_s as total_s, p.total_o as total_o";

		Result resRelType = tx.execute(query, Map.of("split", splitNum));

		// Iterate over the result of the query
		while(resRelType.hasNext()){

			// Get rank_s and rank_o
			Map<String, Object> nextTriple = resRelType.next();
//			System.out.println(nextTriple);
			BigDecimal rank_s = new BigDecimal(nextTriple.get("rank_s").toString());
			BigDecimal rank_o = new BigDecimal(nextTriple.get("rank_o").toString());
			BigDecimal total_s = new BigDecimal(nextTriple.get("total_s").toString());
			BigDecimal total_o = new BigDecimal(nextTriple.get("total_o").toString());

			// Increment total ranks by 2
			numR = numR.add(new BigDecimal(2));

			// MR : Increment mr by both ranks to store the total in mr
			mr = mr.add(rank_s);
			mr = mr.add(rank_o);

			// MRR
			mrr = mrr.add(new BigDecimal(1).divide(rank_s, MathContext.DECIMAL128));
			mrr = mrr.add(new BigDecimal(1).divide(rank_o, MathContext.DECIMAL128));



			// AMR
			amr = amr.add((total_s.add(new BigDecimal(1))).divide(new BigDecimal(2), MathContext.DECIMAL128));
			amr = amr.add((total_o.add(new BigDecimal(1))).divide(new BigDecimal(2), MathContext.DECIMAL128));

		}

		// MR : Divide total of mr by total of numRanks
		mr = mr.divide(numR, MathContext.DECIMAL128);

		// MRR : Divide total of mrr by total of numRanks
		mrr = mrr.divide(numR, MathContext.DECIMAL128);


		// AMR : Divide total of amr by total of numRanks and calculate amr
		amr = amr.divide(numR, MathContext.DECIMAL128);
		amr = new BigDecimal(1).subtract(mr.divide(amr, MathContext.DECIMAL128));

		// Close the previous transaction and result
		tx.close();
		resRelType.close();

		// Find the first triple (minimum id) in the split
		Transaction tx1 = db.beginTx();
		String query1 = "MATCH ()-[p]->() \n" +
				"WHERE p.split = $split \n" +
				"RETURN MIN(id(p)) AS id";

		Result resRelType1 = tx1.execute(query1, Map.of("split", splitNum));
		Map<String, Object> resId = resRelType1.next();
		long minId = (long) resId.get("id");

		// Close the second transaction and result
		tx1.close();
		resRelType1.close();

		// Open transaction for mr, mrr and amr
		Transaction txSetMrMrrAmr = db.beginTx();
		String querySetMrMrrAmr = "MATCH ()-[p]->() \n" +
				"WHERE id(p) = $minId \n" +
				"SET p += {mr: $Valmr, mrr: $Valmrr, amr: $Valamr}";
		txSetMrMrrAmr.execute(querySetMrMrrAmr,
				Map.of(
						"minId", minId,
						"Valmr", mr.toString(),
						"Valmrr", mrr.toString(),
						"Valamr", amr.toString()
				)
		);
		txSetMrMrrAmr.commit();
		txSetMrMrrAmr.close();

		Transaction txSetHitsAtK = db.beginTx();
		StringBuilder querySetHitsAtK = new StringBuilder("MATCH ()-[p]->() \n" +
				"WHERE id(p) = $minId \n" +
				"SET p += {");
		Map<String, Object> queryMap = new HashMap<>();
		queryMap.put("minId", minId);

		for(int i=1; i < hitsAtK.length; i++) {
			querySetHitsAtK.append("hat")
					.append(i)
					.append(":")
					.append(" $Valhit")
					.append(i);

			// Not reached the last element
			if(i < hitsAtK.length - 1) {
				querySetHitsAtK.append(", ");
			}

			queryMap.put("Valhit" + i, hitsAtK[i].toString());

		}
		querySetHitsAtK.append("}");

		txSetHitsAtK.execute(querySetHitsAtK.toString(), queryMap);
		txSetHitsAtK.commit();
		txSetHitsAtK.close();

		service.shutdown();

		System.out.println(new Date() + " -- Done");
	}

}
