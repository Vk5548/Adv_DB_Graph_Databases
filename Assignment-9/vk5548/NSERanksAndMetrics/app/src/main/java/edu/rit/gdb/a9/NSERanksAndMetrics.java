package edu.rit.gdb.a9;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.*;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class NSERanksAndMetrics {
	
	private enum SPLIT {TRAIN, VALID, TEST};
	public enum DISTANCE {L1, L2};
	
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final DISTANCE distance = DISTANCE.valueOf(args[1]); // Either L1 or L2.
		final SPLIT split = SPLIT.values()[Integer.valueOf(args[2])]; // Either VALID or TEST.
		
		System.out.println(new Date() + " -- Started");
		System.out.println("neo4jFolder : "+ neo4jFolder);
		System.out.println("distance : "+ distance);
		System.out.println("split :"+ split.ordinal());
		File neo4jDB = new File(neo4jFolder);
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4jDB.toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
//		Transaction tx = db.beginTx();
		Transaction tx = null, txInner = null;
		
		// TODO We will compute ranks for all the triples in the split using only NSE negatives. For a given p, you can compute and store
		//	in memory all its subjects and objects. For a given triple (s, p, o), subject corruption will be (s', p, o) such that s' in 
		//	Objects(p, Gx); similarly, object corruption will be (s, p, o') such that o' in Subjects(p, Gx). Having (s', p, o), compute
		//	its distance and determine the rank of (s, p, o) based on these distances. Similarly for (s, p, o'). Each triple in the split
		//	will contain four properties: rank_s, rank_o, total_s, total_o. Then, the triple in the split with the minimum id will also
		//	have three more properties: mr, mrr and amr, computed as MR, MRR and AMR, respectively.

		//RETURNING ALL THE NODE WITH THEIR EMBEDDINGS
		String getAllNodes = "MATCH (s)\n" +
				"RETURN id(s) as s, s.embedding as embedding";
		tx = db.beginTx();
		Result allTriplets = tx.execute(getAllNodes);
		Map<Long, BigDecimal[]> allEmbeddings = new HashMap<>();
		while (allTriplets.hasNext()) {
			Map<String, Object> map = allTriplets.next();
			String[] nodeEmbeddingStr = (String[]) map.get("embedding");
			Long nodeID = Long.valueOf(map.get("s").toString());
			BigDecimal[] nodeEmbedding = new BigDecimal[nodeEmbeddingStr.length];
//
			for (int i = 0; i < nodeEmbeddingStr.length; i++) {
				nodeEmbedding[i] = new BigDecimal(nodeEmbeddingStr[i], MathContext.DECIMAL128);
			}

			allEmbeddings.put(nodeID, nodeEmbedding);
		}
		allTriplets.close();
		tx.close();

		//get the type(R)
		String allPredicates = "MATCH () -[r]- ()\n" +
				"WITH  type(r) as connectionType\n" +
				"RETURN connectionType";
		tx = db.beginTx();
		Result predicates = tx.execute(allPredicates);
		Set<String> predicateSet = new HashSet<>();
		while (predicates.hasNext()) {
			predicateSet.add(predicates.next().get("connectionType").toString());
		}
		predicates.close();
		tx.close();
		System.out.println("predicateSet : " + predicateSet);

		//TODO: MAIN LOOP : COPIED FROM LAST ASSIGNMENT
		//iterating over each predicate set
		for (String p : predicateSet) {
			int  totalS =0, totalO = 0;
			BigDecimal rankS = BigDecimal.ZERO, rankO = BigDecimal.ZERO;
			//get the embedding of p
			tx = db.beginTx();
			BigDecimal[] embeddingP = getEmbeddingP(p, tx);
			tx.close();

			//get all the subjects for this p
			Set<Long> subjects = new HashSet<>();
			String getAllSubjects = "MATCH (s) -[r]-> ()\n" +
					"WHERE type(r) = $p\n" +
					"RETURN id(s) as id";
			tx = db.beginTx();
			Result resAllSubjects = tx.execute(getAllSubjects, Map.of("p", p));
			while (resAllSubjects.hasNext()){
				subjects.add((Long) resAllSubjects.next().get("id"));
			}
			resAllSubjects.close();
			tx.close();

			//get all teh objects for this p
			Set<Long> objects = new HashSet<>();
			String getAllObjects = "MATCH () -[r]-> (o)\n" +
					"WHERE type(r) = $p\n" +
					"RETURN id(o) as id";
			tx = db.beginTx();
			Result resAllObjects = tx.execute(getAllObjects, Map.of("p", p));
			while (resAllObjects.hasNext()){
				objects.add((Long) resAllObjects.next().get("id"));
			}
			resAllObjects.close();
			tx.close();

			// TODO For each triple (s, p, o) to evaluate, we will corrupt subjects and objects, that is, (s', p, o) and (s, p, o').
			//	Compute the score of (s, p, o) using the actual distance (L1 or L2). For every (s', p, o), compute its approximate
			//	score and compare to the actual score. Compute rank_s and total_s. Similarly, for every (s, p, o'), compute its
			//	approximate score and compare to the actual score. Compute rank_o and total_o.

			//TODO : sub part 1: get all the triples in split:

			//get the triplets in split
			String cypherTriplesInSplit = "MATCH (s) -[r]-> (o)\n" +
					"WHERE r.split = $split and type(r) = $p\n" +
					"RETURN id(s) as sp, id(o) as op, id(r) as rp";
			tx = db.beginTx();
			Result resultTripletsInSplit = tx.execute(cypherTriplesInSplit, Map.of("split", split.ordinal(), "p", p));
			//contains all SPrimePositive

			//declaring rLess and rEqual
			int rLess = 1, rEqual = 0;
			while (resultTripletsInSplit.hasNext()) { // for each triplet in Gx
				rLess = 1;
				rEqual = 0;
				totalS = 0;
				totalO = 0;
				rankS = BigDecimal.ZERO;
				rankO = BigDecimal.ZERO;
				Map<String, Object> map = resultTripletsInSplit.next();
				Long s = (Long) map.get("sp");
				Long o = (Long) map.get("op");
				Long r = (Long) map.get("rp");

				//calculate the distance of this triple based on L1 and L2
				// get the embeddings of s, p and o
				BigDecimal[] embeddingS = allEmbeddings.get(s);

				BigDecimal[] embeddingO = allEmbeddings.get(o);

				BigDecimal distanceSplitTriple = null;
				if (distance == DISTANCE.L1) {
					distanceSplitTriple = getDistanceL1(embeddingS, embeddingP, embeddingO);
				} else {
					distanceSplitTriple = getDistanceL2(embeddingS, embeddingP, embeddingO);
				}

				//TODO: CORRUPTING S
				//TODO: get fixed values initially
//				BigDecimal pMinusOSquare = getPMinusOSquare(embeddingP, embeddingO);
				//for each triple, get  positives for sPrime
				String cypherGetSPRimePositiveTriples = "Match (sP) -[r]-> (o)\n" +
						"WHERE type(r) = $p and id(o) = $on and r.split <= $split\n" +
						"Return id(sP) as sPrime";
				txInner = db.beginTx();
				Result resultSPrimePositive = txInner.execute(cypherGetSPRimePositiveTriples, Map.of("p", p, "on", o, "split", split.ordinal()));
				Set<Long> sPrimePositive = new HashSet<>();
				System.out.println("sPrimePositive :" + sPrimePositive.size());
				while (resultSPrimePositive.hasNext()) {
					sPrimePositive.add((Long) resultSPrimePositive.next().get("sPrime"));
				}
				resultSPrimePositive.close();
				txInner.close();
				//got all the positives
				//generating the negatives, got the maxID
				BigDecimal negativeTripleDistance = BigDecimal.ZERO;
				System.out.println("sPrimePositive :" + sPrimePositive.size());
				System.out.println("rLess"+ rLess);
				System.out.println("rEqual"+ rEqual);
				System.out.println("rankS should be 0: " + rankS);
				System.out.println("rankO should be 0: " + rankO);
				System.out.println("totalS should be 0: " + totalS);
				System.out.println("totalO should be 0: " + totalO);
				for (Long i : objects) {
					if (!sPrimePositive.contains(i)) {
						totalS++;
						Long negativeSPrime = i;
						if(distance == DISTANCE.L1){
							negativeTripleDistance = getDistanceL1(allEmbeddings.get(negativeSPrime), embeddingP, embeddingO);
						}else{
							negativeTripleDistance = getDistanceL2(allEmbeddings.get(negativeSPrime), embeddingP, embeddingO);
						}


						if(distanceSplitTriple.compareTo(negativeTripleDistance) > 0)
							rLess ++;
//							rLess = rLess.add(BigDecimal.ONE, MathContext.DECIMAL128);
						if(distanceSplitTriple.compareTo(negativeTripleDistance) == 0)
							rEqual++;
//							rEqual = rEqual.add(BigDecimal.ONE, MathContext.DECIMAL128);
						// therefore, we got our triple. d(-sP, p, o)


					}
				}
				sPrimePositive = new HashSet<>();
				System.out.println("rLess"+ rLess);
				System.out.println("rEqual"+ rEqual);
				System.out.println("rankS should be 0: " + rankS);
				System.out.println("rankO should be 0: " + rankO);
				System.out.println("totalS should be 0: " + totalS);
				System.out.println("totalO should be 0: " + totalO);
				rLess = rLess + rLess + rEqual;
//				rLess = rLess.add(rEqual, MathContext.DECIMAL128);

				rankS = BigDecimal.valueOf(rLess).divide(BigDecimal.valueOf(2), MathContext.DECIMAL128);
				//TODO: CORRUPTING O
//				rLess = BigDecimal.ONE; rEqual = BigDecimal.ZERO;
				rLess = 1;
				rEqual = 0;
				System.out.println("rLess"+ rLess);
				System.out.println("rEqual"+ rEqual);
				System.out.println("rankS should be 0: " + rankS);
				System.out.println("rankO should be 0: " + rankO);
				System.out.println("totalS should be 0: " + totalS);
				System.out.println("totalO should be 0: " + totalO);

				//for each triple, get  positives for sPrime
				String cypherGetOPRimePositiveTriples = "Match (s) -[r]-> (oP)\n" +
						"WHERE type(r) = $p and id(s) = $so and r.split <= $split\n" +
						"Return id(oP) as oPrime";
				txInner = db.beginTx();
				Result resultOPrimePositive = txInner.execute(cypherGetOPRimePositiveTriples, Map.of("p", p, "so", s, "split", split.ordinal()));
				Set<Long> oPrimePositive = new HashSet<>();
				System.out.println("oPrimePositive :" + oPrimePositive.size());
				while (resultOPrimePositive.hasNext()) {
					oPrimePositive.add((Long) resultOPrimePositive.next().get("oPrime"));
				}
				System.out.println("oPrimePositive :" + oPrimePositive.size());
				resultOPrimePositive.close();
				txInner.close();


				for (Long i : subjects) {
					if (!oPrimePositive.contains(i)) {
						totalO++;
						Long negativeOPrime = i;

						if(distance == DISTANCE.L1){
							negativeTripleDistance = getDistanceL1(embeddingS, embeddingP, allEmbeddings.get(negativeOPrime));
						}else{
							negativeTripleDistance = getDistanceL2(embeddingS, embeddingP, allEmbeddings.get(negativeOPrime));
						}
						if(distanceSplitTriple.compareTo(negativeTripleDistance) > 0)
							rLess++;
//							rLess = rLess.add(BigDecimal.ONE, MathContext.DECIMAL128);
						if(distanceSplitTriple.compareTo(negativeTripleDistance) == 0)
							rEqual++;
//							rEqual = rEqual.add(BigDecimal.ONE, MathContext.DECIMAL128);
						// therefore, we got our triple. d(-sP, p, o)


					}
				}
				oPrimePositive = new HashSet<>();
				System.out.println("rLess"+ rLess);
				System.out.println("rEqual"+ rEqual);
				System.out.println("rankS should be 0: " + rankS);
				System.out.println("rankO should be 0: " + rankO);
				System.out.println("totalS should be 0: " + totalS);
				System.out.println("totalO should be 0: " + totalO);
//				rLess = rLess.add(rLess, MathContext.DECIMAL128);
//				rLess = rLess.add(rEqual, MathContext.DECIMAL128);
				rLess = rLess + rLess + rEqual;
				rankO = BigDecimal.valueOf(rLess).divide(BigDecimal.valueOf(2), MathContext.DECIMAL128);
				System.out.println("rLess"+ rLess);
				System.out.println("rEqual"+ rEqual);
				System.out.println("rankS should be 0: " + rankS);
				System.out.println("rankO should be 0: " + rankO);
				System.out.println("totalS should be 0: " + totalS);
				System.out.println("totalO should be 0: " + totalO);

				String cypherSetAllVariables = "MATCH (s)-[r]->(o)\n" +
						" WHERE  id(r) = $rID\n" +
						"SET r.rank_s = $rankS, r.rank_o = $rankO, r.total_s = $totalS, r.total_o = $totalO";
				txInner = db.beginTx();
				txInner.execute(cypherSetAllVariables,
						Map.of("rankS", rankS.toString(), "rankO", rankO.toString(), "totalS", totalS,
								"totalO", totalO, "rID", r));
				txInner.commit();
				txInner.close();
				System.out.println("----------------------------------------------------------------------------");
				System.out.print("s "+ s);
				System.out.print(", p "+ p);
				System.out.print(", o "+ o);
				System.out.print(", rankS "+ rankS);
				System.out.print(", rankO "+ rankO);
				System.out.print(", totalS "+ totalS);
				System.out.println(", totalO "+ totalO);
				System.out.println("----------------------------------------------------------------------------");
			}

			resultTripletsInSplit.close();
		}

		//TODO: SECOND PART

		// Initialize variables
		int splitNum = split.ordinal();



		BigDecimal numR = new BigDecimal(0);
		BigDecimal mr = new BigDecimal(0);
		BigDecimal mrr = new BigDecimal(0);
//		BigDecimal[] hitsAtK = new BigDecimal[maxK+1]; // hitsAtK will be at position k-1 in this array
//		for(int i=0; i < hitsAtK.length; i++) {
//			hitsAtK[i] = new BigDecimal(0);
//		}
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



		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}

	private static BigDecimal[] getEmbeddingP(String p, Transaction tx){
		String embedding = "MATCH () -[r]- ()\n" +
				"WHERE type(r) = $p\n" +
				"RETURN  min(id(r)) as min\n" ;

		Long  min = Long.valueOf(tx.execute(embedding, Map.of("p", p)).next().get("min").toString());
		String next = "MATCH () -[r]- ()\n" +
				"WHERE id(r) = $min and type(r) = $p\n" +
				"RETURN r.embedding as embedding ";
		Result resEmbedding = tx.execute(next, Map.of("min", min, "p", p));
		String[] embeddingStr = null;
		if(resEmbedding.hasNext()){
			embeddingStr = (String[]) resEmbedding.next().get("embedding");
		}
		BigDecimal[] embeddingArr = new BigDecimal[embeddingStr.length];
		for(int i =0; i < embeddingStr.length; i++){
			embeddingArr[i] = new BigDecimal(embeddingStr[i], MathContext.DECIMAL128);
		}
		return embeddingArr;
	}

	private static BigDecimal getDistanceL1(BigDecimal[] s, BigDecimal[] p, BigDecimal[] o ){
		//s + p -o
		int length = s.length;
		BigDecimal distance = BigDecimal.ZERO;
		for(int i =0; i < length ; i++){
			BigDecimal add = s[i].add(p[i], MathContext.DECIMAL128);
			BigDecimal sub = add.subtract(o[i], MathContext.DECIMAL128);
			distance = distance.add(sub.abs(MathContext.DECIMAL128), MathContext.DECIMAL128);
		}
		return distance;
	}
	private static BigDecimal getDistanceL2(BigDecimal[] sp, BigDecimal[] p, BigDecimal[] op ){
		int length = sp.length;
		BigDecimal distance = BigDecimal.ZERO;
		for(int i =0; i < length ; i++){
			BigDecimal add = sp[i].add(p[i], MathContext.DECIMAL128);
			BigDecimal sub = add.subtract(op[i], MathContext.DECIMAL128);
			sub = sub.pow(2, MathContext.DECIMAL128);
			distance = distance.add(sub, MathContext.DECIMAL128);
		}
		distance = distance.sqrt(MathContext.DECIMAL128);
		return distance;

	}

}
