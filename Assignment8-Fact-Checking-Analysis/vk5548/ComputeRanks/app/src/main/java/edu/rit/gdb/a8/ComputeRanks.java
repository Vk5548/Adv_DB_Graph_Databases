package edu.rit.gdb.a8;

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

public class ComputeRanks {
	
	private enum SPLIT {TRAIN, VALID, TEST};
	public enum DISTANCE {L1, L2};
	
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final DISTANCE distance = DISTANCE.valueOf(args[1]); // Either L1 or L2.
		final SPLIT split = SPLIT.values()[Integer.valueOf(args[2])]; // Either VALID or TEST.

		System.out.println(new Date() + " -- Started");
		System.out.println("neo4jFolder : " + neo4jFolder);
		System.out.println("distance : " + distance);
		System.out.println("split : " + split.ordinal());

		File neo4jDB = new File(neo4jFolder);

		DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4jDB.toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		Transaction tx = null, txInner = null;

		//get the maximumId in graph
		Long maxID = null;
		String cypherGetMaxID = "Match (u)\n" +
				"RETURN max(id(u)) as maxID";
		tx = db.beginTx();
		Result resultMaxID = tx.execute(cypherGetMaxID);
		if (resultMaxID.hasNext()) {
			maxID = (Long) resultMaxID.next().get("maxID");
		}
		resultMaxID.close();
		tx.close();
		// TODO Your code here!!!!
		// The evaluation task is computationally expensive. We will try to reduce its complexity. To do so, we are going to use 
		//	the dot product of two vectors. Computing the dot product is also computationally intensive, so we will approximate
		//	it using the L1-norms of the embeddings. Thus, first, compute the L1-norm of every node embedding and save it in the
		//	database. The approximation takes the sign of the embeddings, that is, the sign of each position in a given embedding.
		//	Store those in the 'signEmbeddings' map.

		Map<Long, Boolean[]> signEmbeddings = new HashMap<>(); // node and its sign embedding
		// TODO Compute L1-norm and sign embeddings.
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
			Boolean[] signArray = new Boolean[nodeEmbeddingStr.length];
			for (int i = 0; i < nodeEmbeddingStr.length; i++) {
				nodeEmbedding[i] = new BigDecimal(nodeEmbeddingStr[i], MathContext.DECIMAL128);
				//getting signEmbeddings
				signArray[i] = nodeEmbeddingStr[i].startsWith("-") ? false : true;
			}
			signEmbeddings.put(nodeID, signArray);
			allEmbeddings.put(nodeID, nodeEmbedding);
		}
		allTriplets.close();
		tx.close();

		//SETTING THE lOneNorm for all the node embeddings
		for (Map.Entry<Long, BigDecimal[]> entry : allEmbeddings.entrySet()) {
			Long nodeID = entry.getKey();
			BigDecimal[] embedding = entry.getValue();

			BigDecimal lOneNorm = normalize(embedding); // this is lOneNorm, ignore the name of the function
//			System.out.print("nodeID :" + nodeID);
//			System.out.println(", lOneNorm :" + lOneNorm);
			String setNormalizedEmbedding = "MATCH (u)\n" +
					"WHERE id(u) = $nodeID\n" +
					"SET u.lOneNorm = $lOneNorm";
			tx = db.beginTx();
//			String[] embeddindStr = new String[embedding.length];
//			for (int i = 0; i < embedding.length; i++) {
//				embeddindStr[i] = embedding[i].toString();
//			}
			tx.execute(setNormalizedEmbedding, Map.of("nodeID", nodeID, "lOneNorm", lOneNorm.toString()));
			tx.commit();
			tx.close();
		}

		// TODO We will group each triple in the split to be evaluated by predicate (relationship type).
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
		// get the embedding of all p's
		// TODO For each predicate, compute <e, p>, that is, the actual dot product between each node embedding and the embedding
		//	of the current predicate. Save this result in the database.
		//HashMap to store predicate and its embedding, NO NEED FOR THIS NOW
//		Map<String, BigDecimal[]> predicateEmbeddings = new HashMap<>();
		for (String p : predicateSet) {
			int rankS = 0, rankO = 0, totalS =0, totalO = 0;
			//get the embedding of p
			tx = db.beginTx();
			BigDecimal[] embeddingP = getEmbeddingP(p, tx);
			// put the corresponding embedding into the hash map
			// we won't need this anymore
//			predicateEmbeddings.put(p, embeddingP);
			tx.close();
			String getCorrespondingNodes = "Match (u) -[r]- ()\n" +
//					"WHERE type(r) = $p\n" +
					"Return distinct(id(u)) as u"; // distinct can be added here
			tx = db.beginTx();
			Result nodeEmbeddings = tx.execute(getCorrespondingNodes, Map.of("p", p));
			BigDecimal[] embeddingNode = null;
			Long nodeID = null;
			while (nodeEmbeddings.hasNext()) {


				nodeID = (Long) nodeEmbeddings.next().get("u");
				embeddingNode = allEmbeddings.get(nodeID);
				BigDecimal dotProduct = calculateDotProduct(embeddingNode, embeddingP);
//				System.out.print("nodeID :" + nodeID);
//				System.out.println(", dotProduct :" + dotProduct);
				String setDotProduct = "Match (u) \n" +
						"WHERE id(u) = $nodeID\n" +
						"SET u.dotProduct = $dotProduct";
				Transaction txSet = db.beginTx();
				txSet.execute(setDotProduct, Map.of("nodeID", nodeID, "dotProduct", dotProduct.toString()));
				txSet.commit();
				txSet.close();
			}
			nodeEmbeddings.close();
			tx.close();
//			BigDecimal[] embeddingNode = convertStringToBigDecimal(embeddingNStr);

//		}

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
			while (resultTripletsInSplit.hasNext()) {
				rLess = 1; rEqual = 0;
				totalS = 0;
				totalO = 0;
				rankS = 0;
				rankO = 0;
				Map<String, Object> map = resultTripletsInSplit.next();
				Long s = (Long) map.get("sp");
				Long o = (Long) map.get("op");
				Long r = (Long) map.get("rp");
				//we have this already
	//			String p = (String) map.get("p");

				//calculate the distance of this triple based on L1 and L2
				// get the embeddings of s, p and o
				BigDecimal[] embeddingS = allEmbeddings.get(s);
				//We have this again
	//			BigDecimal[] embeddingP = predicateEmbeddings.get(p);
				BigDecimal[] embeddingO = allEmbeddings.get(o);

				BigDecimal distanceSplitTriple = null;
				if (distance == DISTANCE.L1) {
					distanceSplitTriple = getDistanceL1(embeddingS, embeddingP, embeddingO);
				} else {
					distanceSplitTriple = getDistanceL2(embeddingS, embeddingP, embeddingO);
				}

				//TODO: CORRUPTING S
				//TODO: get fixed values initially
				BigDecimal pMinusOSquare = getPMinusOSquare(embeddingP, embeddingO);
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
				for (long i = 0l; i < maxID; i++) {
					if (!sPrimePositive.contains(i)) {
						totalS++;
						Long negativeSPrime = i;
						txInner = db.beginTx();
						negativeTripleDistance = getApproximateDistanceL2(negativeSPrime, allEmbeddings.get(negativeSPrime), p, embeddingP, o, embeddingO, txInner, signEmbeddings, pMinusOSquare);
						txInner.close();
						if(distance == DISTANCE.L1){
							negativeTripleDistance = getApproximateDistanceL1(negativeTripleDistance);
						}
						if(distanceSplitTriple.compareTo(negativeTripleDistance) > 0)
							rLess++;
						if(distanceSplitTriple.compareTo(negativeTripleDistance) == 0)
							rEqual++;
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
				rankS = (rLess + rLess + rEqual)/2;
				//TODO: CORRUPTING O
				rLess = 1; rEqual = 0;
				System.out.println("rLess"+ rLess);
				System.out.println("rEqual"+ rEqual);
				System.out.println("rankS should be 0: " + rankS);
				System.out.println("rankO should be 0: " + rankO);
				System.out.println("totalS should be 0: " + totalS);
				System.out.println("totalO should be 0: " + totalO);
				//FIXED values SPLusPSquare
				BigDecimal sPlusPSquare = getSPlusPSquare(embeddingS, embeddingP);
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


				for (long i = 0l; i < maxID; i++) {
					if (!oPrimePositive.contains(i)) {
						totalO++;
						Long negativeOPrime = i;
						txInner = db.beginTx();
						negativeTripleDistance = getApproximateDistanceL2ForOPrime(s, allEmbeddings.get(s), p, embeddingP, negativeOPrime, allEmbeddings.get(negativeOPrime), txInner, signEmbeddings, sPlusPSquare);
						txInner.close();
						if(distance == DISTANCE.L1){
							negativeTripleDistance = getApproximateDistanceL1(negativeTripleDistance);
						}
						if(distanceSplitTriple.compareTo(negativeTripleDistance) > 0)
							rLess += 1;
						if(distanceSplitTriple.compareTo(negativeTripleDistance) == 0)
							rEqual += 1;
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
				rankO = (rLess + rLess + rEqual)/2;
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
						Map.of("rankS", rankS, "rankO", rankO, "totalS", totalS,
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

		tx.close();
		// TODO There is no easy approximation to compute L1 distance. We will compute the approximate L2 distance and "correct" 
		//	it using the approximate below.
		//	(see https://math.stackexchange.com/questions/2877479/average-ratio-of-manhattan-distance-to-euclidean-distance)
//		BigDecimal l1Correction = BigDecimal.valueOf(4l).divide(new BigDecimal(""+Math.PI), MathContext.DECIMAL128);

		// TODO Save rank_s and rank_o in the database as String (from a BigDecimal) and total_s and total_o as int.
		
		// TODO End of your code.
		
		tx.close();
		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}

	private static BigDecimal normalize(BigDecimal[] e){
		BigDecimal distance = BigDecimal.ZERO;

		for(int i = 0; i < e.length; i++){
			distance = distance.add(e[i].abs(), MathContext.DECIMAL128) ;
		}
		return distance;
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

	private static BigDecimal calculateDotProduct(BigDecimal[] node, BigDecimal[] p){
		BigDecimal dotProduct = BigDecimal.ZERO;
		for(int i =0; i < node.length; i++){
			dotProduct = dotProduct.add(node[i].multiply(p[i], MathContext.DECIMAL128), MathContext.DECIMAL128);
		}
		return dotProduct;
	}

	private static BigDecimal[] convertStringToBigDecimal(String[] arr){
		BigDecimal[] result = new BigDecimal[arr.length];
		for(int i =0; i < arr.length; i++){
			result[i] = new BigDecimal(arr[i], MathContext.DECIMAL128);
		}
		return result;
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
		distance= distance.pow(2, MathContext.DECIMAL128);
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
//		distance = distance.sqrt(MathContext.DECIMAL128);
		return distance;

	}

	private static BigDecimal getApproximateDistanceL2(long s, BigDecimal[] embeddingS, String p, BigDecimal[] embeddingP, long o,
													   BigDecimal[] embeddingO, Transaction tx, Map<Long,
			Boolean[]> signEmbeddings, BigDecimal pMinusOSquare){
		int length = embeddingS.length;
		BigDecimal distance = BigDecimal.ZERO;


		// TODO : 1 + (p -o)^2 + 2 sipi - 2 si oi
		distance = distance.add(BigDecimal.ONE, MathContext.DECIMAL128);
		//(p -o)^2
		distance = distance.add(pMinusOSquare, MathContext.DECIMAL128);
		//2 sipi
		BigDecimal dotProductSP = getSPProduct(s, tx);
		distance = distance.add(dotProductSP, MathContext.DECIMAL128);
		//2 si oi
		BigDecimal productSO = getSOProduct(signEmbeddings, s, o, embeddingS.length, tx);
		distance = distance.subtract(productSO, MathContext.DECIMAL128);
		return distance;

		//TODO : 1 + (s +p)^2 - 2 sioi - 2 si pi

	}

	private static BigDecimal getApproximateDistanceL2ForOPrime(long s, BigDecimal[] embeddingS, String p, BigDecimal[] embeddingP, long o,
													   BigDecimal[] embeddingO, Transaction tx, Map<Long,
			Boolean[]> signEmbeddings, BigDecimal sPlusPSquare){
		int length = embeddingS.length;
		BigDecimal distance = BigDecimal.ZERO;


		//TODO : 1 + (s +p)^2 - 2 sioi - 2 si pi
		distance = distance.add(BigDecimal.ONE, MathContext.DECIMAL128);
		//(p -o)^2
		distance = distance.add(sPlusPSquare, MathContext.DECIMAL128); // sPlusPSqaure
		//2 sipi
		BigDecimal dotProductSP = getSPProduct(o, tx); // for product sioi
		distance = distance.subtract(dotProductSP, MathContext.DECIMAL128);
		//2 si oi
		BigDecimal productSO = getSOProduct(signEmbeddings, s, o, embeddingS.length, tx);
		distance = distance.subtract(productSO, MathContext.DECIMAL128);
		return distance;



	}

	private static BigDecimal getPMinusOSquare(BigDecimal[] p, BigDecimal[] o){
		BigDecimal distance = BigDecimal.ZERO;
		for(int i =0; i < p.length; i++){
			BigDecimal val = p[i].subtract(o[i], MathContext.DECIMAL128).pow(2, MathContext.DECIMAL128);
			distance = distance.add(val, MathContext.DECIMAL128);
		}
		return distance;
	}

	private static BigDecimal getSPProduct(Long s, Transaction tx){

//		System.out.println("getSPProduct  nodeiD " + s);
		String cypherGetDotProduct = "Match (u)\n" +
				"WHERE id(u) = $id\n" +
				"RETURN u.dotProduct as dotProduct";
		BigDecimal dotProduct = new BigDecimal(tx.execute(cypherGetDotProduct, Map.of("id", s)).next().get("dotProduct").toString()) ;
		dotProduct =dotProduct.multiply(new BigDecimal(2), MathContext.DECIMAL128);

		return dotProduct;
	}

	private static BigDecimal getSOProduct(Map<Long, Boolean[]> signEmbeddings, Long s, Long o, int dim, Transaction tx){
		//TODO:
		Boolean[] signS = signEmbeddings.get(s);
		Boolean[] signO = signEmbeddings.get(o);

		int signProduct = 0;
		for(int i = 0; i < dim; i++){
			signProduct += (signS[i]==false?-1:1) * (signO[i]==false?-1:1);
		}
		//get the normalized value of s and o from database
		String cypherGetLOneNorm = "MATCH (s)\n" +
				"WHERE id(s) = $id\n" +
				"RETURN s.lOneNorm as lOneNorm";
//		String lOne = tx.execute(cypherGetLOneNorm, Map.of("id", s)).toString();
//		System.out.println("lOne : " +lOne);

		BigDecimal lOneS = new BigDecimal(tx.execute(cypherGetLOneNorm, Map.of("id", s)).next().get("lOneNorm").toString());
		lOneS = lOneS.divide(BigDecimal.valueOf(dim), MathContext.DECIMAL128);
		BigDecimal lOneO = new BigDecimal(tx.execute(cypherGetLOneNorm, Map.of("id", o)).next().get("lOneNorm").toString());
		lOneO = lOneO.divide(BigDecimal.valueOf(dim), MathContext.DECIMAL128);

		lOneS = lOneS.multiply(lOneO, MathContext.DECIMAL128);
		lOneS = lOneS.multiply(new BigDecimal(signProduct), MathContext.DECIMAL128);
		lOneS = lOneS.multiply(new BigDecimal(2), MathContext.DECIMAL128);
		return lOneS;
	}

	private static BigDecimal getApproximateDistanceL1(BigDecimal distance){
		BigDecimal l1Correction = BigDecimal.valueOf(4l).divide(new BigDecimal(""+Math.PI), MathContext.DECIMAL128);
		distance = distance.multiply(l1Correction, MathContext.DECIMAL128);
		return distance;
	}

	private static BigDecimal getSPlusPSquare(BigDecimal[] s, BigDecimal[] p){
		BigDecimal distance = BigDecimal.ZERO;
		for(int i = 0; i < s.length; i++){
			BigDecimal val = s[i].add(p[i], MathContext.DECIMAL128).pow(2, MathContext.DECIMAL128);
			distance = distance.add(val, MathContext.DECIMAL128);
		}
		return distance;
	}
}
