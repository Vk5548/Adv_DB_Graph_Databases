package edu.rit.gdb.a7;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
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

public class TripleScoring {
	
	public enum DISTANCE {L1, L2};
	
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final DISTANCE distance = DISTANCE.valueOf(args[1]); // Either L1 or L2.
		
		System.out.println(new Date() + " -- Started");
		System.out.println("neo4jFolder: " + neo4jFolder);
		System.out.println("distance: " + distance);
		File neo4jDB = new File(neo4jFolder);
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4jDB.toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		Transaction tx = null;
		
		// TODO For each triple (edge), compute its distance and attach it to the edge as 'score.'

		//this map contains the predicate type and number of triples present
		Map<String, BigDecimal[]> originalPredicateCount = new HashMap<>();
		String getRelationTypes = "MATCH () -[r]- ()\n" +
				"WITH  type(r) as connectionType\n" +
				"RETURN connectionType"; //, count(connectionType) as count
		tx = db.beginTx();
		Result resRelType = tx.execute(getRelationTypes);
//		int count =-1;
		while(resRelType.hasNext()){
			Map<String, Object> resMap = resRelType.next();
			String predicateType = resMap.get("connectionType").toString();
//			Integer count = Integer.parseInt(resMap.get("c ount").toString());
			originalPredicateCount.put(predicateType, new BigDecimal[]{BigDecimal.ZERO});
		}
		tx.close();
		resRelType.close();
		System.out.println("originalPredicateCount : "+ originalPredicateCount);
		for(String p : originalPredicateCount.keySet()){
			//call the function
			tx = db.beginTx();
			BigDecimal[] arr = getEmbeddingP(p, tx);
			originalPredicateCount.put(p, arr);
			tx.close();
		}
		System.out.println("originalPredicateCount : "+ originalPredicateCount);
		//Todo: iterating over every spo

		//Store all the edges in map and source
		Map<Long, BigDecimal> edgeSourceMap = new HashMap<>();
		for(String p : originalPredicateCount.keySet()){
			String getTripletsCurrentPredicate = "Match (s) -[r]-> (o)\n" +
					"WHERE type(r) = $p\n" +
					"RETURN id(s) as s, id(o) as o, id(r) as r";

			tx = db.beginTx();
			Result resultTriplets = tx.execute(getTripletsCurrentPredicate, Map.of("p", p));
			Transaction inner = null;
			while (resultTriplets.hasNext()){
				Map<String, Object> nodeMap = resultTriplets.next();
				Long subject = Long.valueOf(nodeMap.get("s").toString());
				Long object = Long.valueOf(nodeMap.get("o").toString());
				Long predicate = Long.valueOf(nodeMap.get("r").toString());
				//get embedding of nodes
				inner = db.beginTx();
				BigDecimal[] embeddingS = getEmbedding(subject, inner);
				inner.close();
				inner = db.beginTx();
				BigDecimal[] embeddingO = getEmbedding(object, inner);
				inner.close();
				BigDecimal distTriple = BigDecimal.ZERO;
				if(distance == DISTANCE.L1){
					distTriple = getDistanceL1(embeddingS, originalPredicateCount.get(p), embeddingO);
				}else {
					distTriple = getDistanceL2(embeddingS, originalPredicateCount.get(p), embeddingO);
				}
				edgeSourceMap.put(predicate, distTriple);
			}
			resultTriplets.close();
			tx.close();
		}
		service.shutdown();
		
		BatchInserter inserter = BatchInserters.inserter(DatabaseLayout.of(
				Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, neo4jDB.toPath()).build()));

		for(Long edgeID : edgeSourceMap.keySet()){
			inserter.setRelationshipProperty(edgeID, "score", edgeSourceMap.get(edgeID).toString());

		}
		inserter.shutdown();
		
		System.out.println(new Date() + " -- Done");
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
	private static BigDecimal[] getEmbeddingP(String p, Transaction tx){
		String embedding = "MATCH () -[r]- ()\n" +
				"WHERE type(r) = $p\n" +
				"RETURN  min(id(r)) as min\n" ;

		Long  min = Long.valueOf(tx.execute(embedding, Map.of("p", p)).next().get("min").toString());
		String next = "MATCH () -[r]- ()\n"+
				"WHERE id(r) = $min AND type(r) = $p\n" +
				"RETURN r.embedding as embedding ";
		Result resEmbedding = tx.execute(next, Map.of("min", min, "p",p));
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
	private static BigDecimal[] getEmbedding(Long nodeID, Transaction tx){
		String embedding = "MATCH (n)\n" +
				"WHERE id(n) = $nodeID\n" +
				"RETURN n.embedding as embedding";

		Result resEmbedding = tx.execute(embedding, Map.of("nodeID", nodeID));
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
}
