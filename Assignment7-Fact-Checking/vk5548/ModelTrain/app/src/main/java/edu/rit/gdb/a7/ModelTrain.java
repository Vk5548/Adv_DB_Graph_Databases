package edu.rit.gdb.a7;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class ModelTrain {

	private enum SPLIT {TRAIN, VALID, TEST};
	public enum DISTANCE {L1, L2};

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final int dim = Integer.valueOf(args[1]), batchSize = Integer.valueOf(args[2]), negativeRate = Integer.valueOf(args[3]), totalEpochs = Integer.valueOf(args[4]);
		final BigDecimal alpha = new BigDecimal(args[5]), gamma = new BigDecimal(args[6]);
		final DISTANCE distance = DISTANCE.valueOf(args[7]); // Either L1 or L2

		BigDecimal DIM_SQRT = getSqrt(dim);
		BigDecimal[] defaultEmbeddingVals = getBigDecimalDefault(DIM_SQRT);
		BigDecimal normalizedEmbeddingVal = getNormalizedDefaultEmbedding(defaultEmbeddingVals);

		System.out.println(new Date() + " -- Started");

		File neo4jDB = new File(neo4jFolder);

		// TODO Your code here!!!!
		// We use GraphDatabaseService to gather ids and, then, rely on the inserter.
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4jDB.toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		Transaction tx = db.beginTx();
		// TODO Get the maximum id of the nodes.
		long maxId = getMaxNodeId(tx);
		tx.close();

		// TODO Get the ids of the triples where the embeddings are stored.
		// The convention is that the embedding of p will be in the relationship with minimum id with type p.
		// Get the maximum id of the relationships.
		Map<String, Long> embeddingsOfPredicates = new HashMap<>();
		String getRelationTypes = "MATCH () -[r]- ()\n" +
				"RETURN type(r) AS connectionType";
		tx = db.beginTx();
		Result resRelType = tx.execute(getRelationTypes);
		while(resRelType.hasNext()){
			Map<String, Object> resMap = resRelType.next();
			String predicateType = resMap.get("connectionType").toString();
//			Integer count = Integer.parseInt(resMap.get("c ount").toString());
			embeddingsOfPredicates.put(predicateType, Long.valueOf("-1"));
		}
		tx.close();
		resRelType.close();
		// TODO Get the last triple id. This is for random selection later on.
		long maxRelId = 0;

		for(String p: embeddingsOfPredicates.keySet()) {
			tx = db.beginTx();
			Long minId = getEmbeddingPId(p, tx);
			if (maxRelId < minId) {
				maxRelId = minId;
			}
			embeddingsOfPredicates.put(p, minId);
			tx.close();
		}

		service.shutdown();

		// Let's start the process.
		BatchInserter inserter = BatchInserters.inserter(DatabaseLayout.of(
				Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, neo4jDB.toPath()).build()));

		// TODO Initialize node embeddings. Use dimension dim and every dimension between -6/sqrt(dim) and 6/sqrt(dim).
		//	Use property 'embedding.' Normalize the embeddings while you initialize them.
		for (long id = 0L; id <= maxId; id++) {
			String[] embeddingStr = new String[dim];
			for(int i =0; i < embeddingStr.length; i++){
				embeddingStr[i] = normalizedEmbeddingVal.toString();
			}

			inserter.setNodeProperty(id, "embedding", embeddingStr);
		}

		// TODO Initialize predicate embeddings. Use dimension dim and every dimension between -6/sqrt(dim) and 6/sqrt(dim).
		//		Use property 'embedding.' Normalize the embeddings while you initialize them.
		for (String p : embeddingsOfPredicates.keySet()) {
			String[] embeddingStr = new String[dim];
			for(int i =0; i < embeddingStr.length; i++){
				embeddingStr[i] = normalizedEmbeddingVal.toString();
			}

			long predicateId = embeddingsOfPredicates.get(p);

			inserter.setRelationshipProperty(predicateId, "embedding", embeddingStr);
		}

		for (int epoch = 0; epoch < totalEpochs; epoch++) {
			BigDecimal totalLossEpoch = BigDecimal.ZERO;
			System.out.println(new Date() + " -- Epoch: " + epoch);

			System.out.println(new Date() + " -- Composing batch...");

			// Store the unique entities in the current batch.
			Set<Long> entitiesInBatch = new HashSet<>();
			List<Tuple> batch = new ArrayList<>();
			String randomRelCypher = "MATCH (s)-[p]->(o) \n" +
					"WHERE p.split = 0 \n" +
					"RETURN id(s) AS sid, id(p) as pid, id(o) AS oid, rand() AS r\n" +
					"ORDER BY r";

			tx = db.beginTx();
			Result randomTriples = tx.execute(getRelationTypes);


			// TODO Compose the batch. Select a random triple and, if it is training, compute negativeReate corruptions.
			while (entitiesInBatch.size() < batchSize) {
				// TODO Pick a random triple in training. Store the entities in entitiesInBatch.
				Long[] triple = new Long[]{0L, 0L, 0L};
				if(randomTriples.hasNext()) {
					Map<String, Object> resMap = randomTriples.next();
					triple[0] = Long.valueOf(resMap.get("sid").toString());
					triple[1] = Long.valueOf(resMap.get("pid").toString());
					triple[2] = Long.valueOf(resMap.get("oid").toString());
				}

				// Corrupt the current triple.
				for (int nr = 0; nr < negativeRate; nr++) {
					Tuple t = null;

					// TODO Get random node.
					// TODO Corrupt either subject or object of the current triple.
					// TODO Check that the corrupted triple does not exist in training.

					// Add to the batch.
					batch.add(t);
					// TODO Store the entities in entitiesInBatch.
				}
			}

			System.out.println(new Date() + " -- Batch is ready! Normalizing and updating gradients...");

			// TODO Normalize entities before gradient.
			for (Long e : entitiesInBatch) {
			}

			// Gradient.
			long updates = 0l;
			for (Tuple t : batch) {
				// TODO Compute l = gamma + d(t.s, t.p, t.o) - d(t.sp, t.p, t.op)
				BigDecimal l = BigDecimal.ZERO;

				if (l.compareTo(BigDecimal.ZERO) > 0) {
					// TODO Update embeddings and store in database.

					totalLossEpoch = totalLossEpoch.add(l);
					updates++;
				}
			}

			System.out.println(new Date() + " -- Epoch is over! Loss: " + totalLossEpoch.divide(BigDecimal.valueOf(updates), MathContext.DECIMAL128));
		}

		System.out.println(new Date() + " -- Done");

		inserter.shutdown();
	}

	private static boolean doesTripleExistInTraining(long sid, long pid, long oid) {

	}

	private static BigDecimal getSqrt(int dim) {
		BigDecimal n = new BigDecimal(dim);
		MathContext mc = new MathContext(10);

		return  n.sqrt(mc);
	}

	private static BigDecimal[] getBigDecimalDefault(BigDecimal SQRT) {
		MathContext mc = new MathContext(10);
		BigDecimal a = new BigDecimal(-6 ).divide(SQRT);
		BigDecimal b = new BigDecimal(6 ).divide(SQRT);

		return new BigDecimal[]{a, b};
	}

	private static BigDecimal getNormalizedDefaultEmbedding(BigDecimal[] defaults) {
		return defaults[0];
	}

	private static Long getEmbeddingPId(String p, Transaction tx){
		String embedding = "MATCH () -[r]- ()\n" +
				"WHERE type(r) = $p\n" +
				"RETURN  min(id(r)) as min\n" ;

		return Long.valueOf(tx.execute(embedding, Map.of("p", p)).next().get("min").toString());
	}

	private static Long getMaxNodeId(Transaction tx){
		String embedding = "MATCH (n) \n" +
				"RETURN max(id(n)) as maxId" ;

		return Long.valueOf(tx.execute(embedding).next().get("maxId").toString());
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

	private class Tuple {
		long s, p, o, sp, op;

		public Tuple(long s, long p, long o, long sp, long op) {
			super();
			this.s = s;
			this.p = p;
			this.o = o;
			this.sp = sp;
			this.op = op;
		}
	}

}