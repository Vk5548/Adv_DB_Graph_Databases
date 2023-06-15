package edu.rit.gdb.a7;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class Epoch {
	
	public enum DISTANCE {L1, L2};
	
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final long s = Long.valueOf(args[1]), o = Long.valueOf(args[3]), 
				sp = Long.valueOf(args[4]), op = Long.valueOf(args[5]);
		final String pStr = args[2];
//		"43978,concept:animaldevelopdisease,4503,4303,43978,1095";
		final BigDecimal alpha = new BigDecimal(args[6]), gamma = new BigDecimal(args[7]);
		final DISTANCE distance = DISTANCE.valueOf(args[8]); // Either L1 or L2
		
		System.out.println(new Date() + " -- Started");
		System.out.println("neo4jFolder := "+ neo4jFolder);
		System.out.println("s := "+ s);
		System.out.println("o := "+ o);
		System.out.println("pStr := "+ pStr);
		System.out.println("sp := "+ sp);
		System.out.println("op := "+ op);
		System.out.println("alpha := "+ alpha);
		System.out.println("gamma := "+ gamma);
		System.out.println("distance := "+ distance);




		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		
		// TODO: Your code here!!!!
		// Having a 5-tuple (s, p, o, sp, op), you need to run one epoch. To do so, you need to assume that each entity (node) has a 
		//	property 'embedding' that is an array of String. Each string value must be interpreted as a BigDecimal.DECIMAL128. Note
		//	that the embedding of p is in the triple that has p as its type and the minimum id. The triple (s, p, o) is positive
		//	and the triple (sp, p, op) is negative. You need to normalize all entity embeddings first (never normalize predicate
		//	embeddings!) and, then, compute and apply the gradients only if gamma + d(s, p, o) - d(sp, p, op) > 0.
		//
		// Important! Note that there is nothing preventing s=o=op or s=o=sp. These cases entail that there is a single embedding
		//	that must be updated. Make sure you use Java pointers to account to these issues.
		//
		// Recall that the embedding of predicate pStr is in the relationship (edge) such that its type is pStr and its id is min.
		
		// TODO: End of your code.
		//get the embedding property
		//if s ==o ==op

		Transaction tx = db.beginTx();
		BigDecimal[] embeddingS = getEmbedding(s, tx);
		int i = 0;
		tx.close();
		for(BigDecimal str: embeddingS){
			System.out.println(i++ +" embeddingS :" + str);
		}
		BigDecimal[] embeddingO = null;
		if(s == o){ //s== o
			embeddingO = embeddingS;
		}else{ //s != o
			tx = db.beginTx();
			embeddingO = getEmbedding(o, tx);
			tx.close();
		}
		i =0;
		for(BigDecimal str: embeddingO){
			System.out.println(i++ +" embeddingO :" + str);
		}
		BigDecimal[] embeddingSP = null;
		if(s==sp && o == sp){ //s==o==sp
			embeddingSP = embeddingS;
		}else if(s == sp){ //s == sp
			embeddingSP = embeddingS;
		}else if(o == sp){ //o == sp
			embeddingSP = embeddingO;
		}
		else {
			tx = db.beginTx();
			embeddingSP = getEmbedding(sp, tx);
			tx.close();

		}
		BigDecimal[] embeddingOP = null;
		if(s==op && op == o){ //if s ==o ==op
			embeddingOP = embeddingS;
		}else if(s==op){// s== op
			embeddingOP = embeddingS;
		}else if(op == o){ // op == o
			embeddingOP = embeddingO;
		} else{
			tx = db.beginTx();
			embeddingOP = getEmbedding(op, tx);
			tx.close();
		}
		//get the embedding of p
		tx = db.beginTx();
		BigDecimal[] embeddingP = getEmbeddingP(pStr, tx);
		tx.close();
		i =0;
		for(BigDecimal str: embeddingP){
			System.out.println(i++ +" embeddingP :" + str);
		}
		// TODO :Normalize all the nodes
		//account for same nodes
		normalize(embeddingS);
		if(s != o)
			normalize(embeddingO);
		if(s != sp && o != sp)
			normalize(embeddingSP);
		if(o != op && op != s)
			normalize(embeddingOP);
		//After getting all the embeddings, need to calculate the distance
		BigDecimal distanceSPO = null, distanceSPPOP;
		if(distance == DISTANCE.L1){
			distanceSPO = getDistanceL1(embeddingS, embeddingP, embeddingO);
			distanceSPPOP = getDistanceL1(embeddingSP, embeddingP, embeddingOP);
		}else{
			distanceSPO = getDistanceL2(embeddingS, embeddingP, embeddingO);
			distanceSPPOP = getDistanceL2(embeddingSP, embeddingP, embeddingOP);
		}
		System.out.println("distanceSPO :" + distanceSPO);
		System.out.println("distanceSPPOP :" + distanceSPPOP);

		//if condition
		//if gamma + d(s, p, o) - d(sp, p, op) > 0.
		BigDecimal ifCondition = gamma.add(distanceSPO, MathContext.DECIMAL128).subtract(distanceSPPOP, MathContext.DECIMAL128);
		if(ifCondition.compareTo(BigDecimal.ZERO) > 0){
			//get vectorX and vectorXP
			BigDecimal[] vectorX = getVectorAddition(embeddingS, embeddingP, embeddingO);
			BigDecimal[] vectorXP = getVectorAddition(embeddingSP, embeddingP, embeddingOP);

				//TODO: updating all the values
				for(int j =0; j < embeddingS.length; j++){
					if(distance == DISTANCE.L1){
						//we need sign (x)
						//s
						embeddingS[j] = embeddingS[j].subtract(alpha.multiply(BigDecimal.valueOf(vectorX[j].signum()),
								MathContext.DECIMAL128), MathContext.DECIMAL128);
						//o
						embeddingO[j] = embeddingO[j].add(alpha.multiply(BigDecimal.valueOf(vectorX[j].signum()),
								MathContext.DECIMAL128), MathContext.DECIMAL128);
						//sp
						embeddingSP[j] = embeddingSP[j].add(alpha.multiply(BigDecimal.valueOf(vectorXP[j].signum()),
								MathContext.DECIMAL128), MathContext.DECIMAL128);
						//op
						embeddingOP[j] = embeddingOP[j].subtract(alpha.multiply(BigDecimal.valueOf(vectorXP[j].signum()),
								MathContext.DECIMAL128), MathContext.DECIMAL128);
						//p
						embeddingP[j] = embeddingP[j].subtract(alpha.multiply(BigDecimal.valueOf(vectorX[j].signum()), MathContext.DECIMAL128),
								MathContext.DECIMAL128).add(alpha.multiply(BigDecimal.valueOf(vectorXP[j].signum()), MathContext.DECIMAL128), MathContext.DECIMAL128);

					}else{
						//we need  (x)
						//s
						embeddingS[j] = embeddingS[j].subtract(alpha.multiply(vectorX[j],
								MathContext.DECIMAL128), MathContext.DECIMAL128);
						//o
						embeddingO[j] = embeddingO[j].add(alpha.multiply(vectorX[j],
								MathContext.DECIMAL128), MathContext.DECIMAL128);
						//sp
						embeddingSP[j] = embeddingSP[j].add(alpha.multiply(vectorXP[j],
								MathContext.DECIMAL128), MathContext.DECIMAL128);
						//op
						embeddingOP[j] = embeddingOP[j].subtract(alpha.multiply(vectorXP[j],
								MathContext.DECIMAL128), MathContext.DECIMAL128);
						//p
						embeddingP[j] = embeddingP[j].subtract(alpha.multiply(vectorX[j], MathContext.DECIMAL128),
								MathContext.DECIMAL128).add(alpha.multiply(vectorXP[j], MathContext.DECIMAL128), MathContext.DECIMAL128);

					}
				}


			//TODO: 	updating all the node property
			tx = db.beginTx();
			updateNodeEmbedding(embeddingS, s, tx);
			tx.close();
			tx = db.beginTx();
			updateNodeEmbedding(embeddingO, o, tx);
			tx.close();
			tx = db.beginTx();
			updateNodeEmbedding(embeddingSP, sp, tx);
			tx.close();
			tx = db.beginTx();
			updateNodeEmbedding(embeddingOP, op, tx);
			tx.close();
			//TODO: updating the property
			tx = db.beginTx();
			updatePredicateEmbedding(embeddingP, pStr, tx);

		}

		//embedding S



		System.out.println("------------------embeddingS--------------------------------------------");
		i = 0;
		for(BigDecimal str: embeddingS){
			System.out.println(i++ +"  :" + str);
		}
		System.out.println("------------------embeddingO--------------------------------------------");
		i = 0;
		for(BigDecimal str: embeddingO){
			System.out.println(i++ +"  :" + str);
		}
		System.out.println("------------------embeddingSP--------------------------------------------");
		i = 0;
		for(BigDecimal str: embeddingSP){
			System.out.println(i++ +"  :" + str);
		}
		System.out.println("------------------embeddingOP--------------------------------------------");
		i = 0;
		for(BigDecimal str: embeddingOP){
			System.out.println(i++ +"  :" + str);
		}
		System.out.println("------------------embeddingP--------------------------------------------");
		i = 0;
		for(BigDecimal str: embeddingP){
			System.out.println(i++ +"  :" + str);
		}


		System.out.println(new Date() + " -- Done");
		
		service.shutdown();
	}

	private static void normalize(BigDecimal[] e){
		BigDecimal distance = BigDecimal.ZERO;

		for(int i = 0; i < e.length; i++){
			distance = distance.add(e[i].pow(2, MathContext.DECIMAL128), MathContext.DECIMAL128) ;
		}
		distance = distance.sqrt(MathContext.DECIMAL128);
		for(int i = 0; i < e.length; i++){
			e[i] = e[i].divide(distance, MathContext.DECIMAL128) ;
		}

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

	private static BigDecimal[] getVectorAddition(BigDecimal[] s, BigDecimal[] p, BigDecimal[] o){
		BigDecimal[] result = new BigDecimal[s.length];
		for(int i =0; i < s.length ; i++){
			BigDecimal add = s[i].add(p[i], MathContext.DECIMAL128);
			BigDecimal sub = add.subtract(o[i], MathContext.DECIMAL128);
			sub = sub.multiply(BigDecimal.valueOf(2), MathContext.DECIMAL128);
//			sub = BigDecimal.valueOf(2).multiply(sub, MathContext.DECIMAL128);
			result[i] = new BigDecimal(String.valueOf(sub), MathContext.DECIMAL128);

		}
		return result;
	}









	private static void updateNodeEmbedding(BigDecimal[] embedding, Long nodeID, Transaction tx){
		String[] embeddingStr = new String[embedding.length];
		for(int i =0; i < embedding.length; i++){
			embeddingStr[i] = embedding[i].toString();
		}
		String cypher = "MATCH (u)\n" +
				"WHERE id(u) = $nodeID\n" +
				"SET u.embedding = $embeddingStr";
		tx.execute(cypher, Map.of("nodeID", nodeID, "embeddingStr", embeddingStr));
		tx.commit();
	}

	private static void updatePredicateEmbedding( BigDecimal[] embedding, String p, Transaction tx){
		String[] embeddingStr = new String[embedding.length];
		for(int i =0; i < embedding.length; i++){
			embeddingStr[i] = embedding[i].toString();
		}
		String update = "MATCH () -[r]- ()\n" +
				"WHERE type(r) = $p\n" +
				"RETURN  min(id(r)) as min\n" ;

		Long  min = Long.valueOf(tx.execute(update, Map.of("p", p)).next().get("min").toString());
		String next = "MATCH () -[r]- ()\n" +
				"WHERE id(r) = $min and type(r) = $p\n" +
				"SET r.embedding = $embeddingStr";
		tx.execute(next, Map.of("min", min, "embeddingStr", embeddingStr, "p", p));
		tx.commit();

	}

}
//4, 6, 7, 8, 10
