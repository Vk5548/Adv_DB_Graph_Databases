package edu.rit.gdb.a7;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import scala.Int;

public class KGSplit {
	
	private enum SPLIT {TRAIN, VALID, TEST}; //{0, 1, 2}
	
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final double tolerance = Double.valueOf(args[1]);
		final double minPercentage = Double.valueOf(args[2]);
		
		System.out.println(new Date() + " -- Started");

		System.out.println("neo4jFolder : " + neo4jFolder);
		System.out.println("tolerance : " + tolerance);
		System.out.println("minPercentage : " + minPercentage);
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		Transaction tx = null;
		Transaction inner = null;
		Transaction second = null;
		
		// TODO: Your code here!!!!
		// Move all triples to training first. Compute the average indegrees and outdegrees of each predicate.
		// For each triple t in training, we evaluate whether it can be moved to either validation or test (one or the
		// other so the sizes are similar). Triple t=(s,p,o) can be removed from training and added to another split
		// if the following three conditions are true: 1) The percentage of triples containing p in training is above
		// minPercentage.
		//	2) s and o cannot be isolated, that is, both s and o have at least one edge that is in training.
		//3) Let ni be the new indegree of p without t in training, and no be the new outdegree of p without t in training.
		// Then,
		//	|ni-i|< tolerance and |no-o|< tolerance, where i and o are the indegrees and outdegrees of p in the whole graph.


		//TODO: preprocessing step: to set .split property of all the triples
		String cypherSetSplit = "Match () -[r]-> ()\n" +
				"SET r.split = 0\n" +
				"Return count(r) as cnt";
		tx = db.beginTx();
		int totalEdges =  Integer.parseInt(tx.execute(cypherSetSplit).next().get("cnt").toString());
		tx.commit();
		tx.close();
		System.out.println("totalEdges : "+ totalEdges);
		//CHECK FOR UPDATE ON OF SPLIT PROPERTY
//		String check = "MATCH (u) -[r]- () \n" +
//				"WHERE id(u) = 0\n" +
//				"Return r.split as split\n" +
//				"Limit 1";
//		tx = db.beginTx();
//		int split =  Integer.parseInt(tx.execute(check).next().get("split").toString());
//		tx.close();
//		System.out.println("split : " + split);
		//STEP : 1
		//GET ALL THE RELATION TYPES:-
		//this map contains the predicate type and number of triples present
		Map<String, Integer> originalPredicateCount = new HashMap<>();
		String getRelationTypes = "MATCH () -[r]- ()\n" +
				"WITH  type(r) as connectionType\n" +
				"RETURN connectionType"; //, count(connectionType) as count
		tx = db.beginTx();
		Result resRelType = tx.execute(getRelationTypes);
		int count =-1;
		while(resRelType.hasNext()){
			Map<String, Object> resMap = resRelType.next();
			String predicateType = resMap.get("connectionType").toString();
//			Integer count = Integer.parseInt(resMap.get("c ount").toString());
			originalPredicateCount.put(predicateType, count);
		}
		tx.close();
		resRelType.close();
		System.out.println("originalPredicateCount: \n" + originalPredicateCount);
		// can be done while iterating through all types of edges -- nevermind
		for(String predicateType: originalPredicateCount.keySet()){
			String getCountType = "MATCH () -[r:"+predicateType +"]-> ()\n" +
					"RETURN COUNT(*) as numberOfTriples";
			tx = db.beginTx();
			int total = Integer.parseInt(tx.execute(getCountType).next().get("numberOfTriples").toString());
			originalPredicateCount.put(predicateType, total);
			tx.close();
		}

		System.out.println("originalPredicateCount: \n" + originalPredicateCount);
//		//need to find the avg indegree and outdegree
		Map<String, Double> origInDegree = new HashMap<>();
		Map<String, Double> origOutDegree = new HashMap<>();
		boolean flag = false;
		int current =-1;
		current = totalEdges;
		//todo: exit condition of the loop
		boolean exitCondition = false;
		for(String p: originalPredicateCount.keySet()){
			//current predicate total edges
			current = originalPredicateCount.get(p);
			int origTotalPredicateEdgesCount = current;
			// get all the nodes i.e just the total such that () -[p]-> (o)
			String inDegreeNodes = "Match () -[r:" + p + "]-> (o)\n" +
					"Return count(DISTINCT o) as in";
			tx = db.beginTx();
			double totalIndegreeNodes = Double.parseDouble(tx.execute(inDegreeNodes).next().get("in").toString());
			double avgIndegree = originalPredicateCount.get(p) / totalIndegreeNodes;
			tx.close();
			//put in map
//			origInDegree.put(p, avgIndegree);
			String outDegreeNodes = "Match (s) -[r:" + p + "]-> ()\n" +
					"Return count(DISTINCT s) as out";
			tx = db.beginTx();
			double totalOutDegreeNodes = Double.parseDouble(tx.execute(outDegreeNodes).next().get("out").toString());
			double avgOutDegree = originalPredicateCount.get(p) / totalOutDegreeNodes;
			tx.close();

			//put in map
//			origOutDegree.put(p, avgOutDegree);
			//todo: we will also need to keep track of current number of edges and the original edges in the graph

			// getting all the edges or all the triplets of this particular predicate type
			String getTripletsCurrentPredicate = "Match (s) -[r:"+ p + "]-> (o)\n" +
					"RETURN id(s) as s, id(o) as o, id(r) as r";
			tx = db.beginTx();
			Result nodesOfTriplets = tx.execute(getTripletsCurrentPredicate);

			while(nodesOfTriplets.hasNext()){
				Map<String, Object> nodeMap = nodesOfTriplets.next();
				Long subject = Long.valueOf(nodeMap.get("s").toString());
				Long object = Long.valueOf(nodeMap.get("o").toString());
				Long predicate = Long.valueOf(nodeMap.get("r").toString());
//				System.out.println("nodeMap.get(s) : "+ nodeMap.get("s") + " \n nodeMap.get(o) : "+ nodeMap.get("o"));
				//for each iteration, we can not consider the current triplet
				//checking the 95-5 thing
				if(current - 1 < minPercentage * origTotalPredicateEdgesCount){//stopping condition
//					exitCondition = true;
					break;
				}
				//todo: ISOLATION CONDITION for subject
				String isolationS = "Match (s) -[r]- ()\n" +
						"WHERE r.split = 0 AND id(s) = $subject AND r is not null and id(r) <> $predicate\n" +
						"RETURN id(s) as s";
				inner = db.beginTx();

				Result isSIsIsolated = inner.execute(isolationS, Map.of("subject", subject, "predicate", predicate));
				//todo: ISOLATION CONDITION for object
				String isolationO = "Match (o) -[r]- ()\n" +
						"WHERE r.split = 0 AND id(o) = $object AND r is not null and id(r) <> $predicate\n" +
						"RETURN id(o) as o";
				Transaction extra = db.beginTx();
				Result isOIsolated = extra.execute(isolationO, Map.of("object", object, "predicate", predicate));
//				if (isSIsIsolated.hasNext()){
//
//				}

				if(isSIsIsolated.hasNext() && isOIsolated.hasNext()){ // make them for both
					//todo: calculate the current indegree and outdegree
					//indegree
					//numerator will be previous - 1
					//and as for denominator, we will have to check for current predicate triplets is any
					String ifCurrentPredicateTriplet = "Match () -[r:"+ p + "]-> (s)\n" +
							"WHERE id(s) = $subject and r is not null AND  id(r) <> $predicate\n" +
							"Return id(s) as s";
					second = db.beginTx();
					Result isCurrentPredicate = second.execute(ifCurrentPredicateTriplet,
							Map.of("subject", subject, "object", object,  "predicate", predicate));


					double avgInDegreePrime = 0.0;
					if(isCurrentPredicate.hasNext()){
						avgInDegreePrime = (originalPredicateCount.get(p) - 1)/totalIndegreeNodes;
					}else{
						avgInDegreePrime = (originalPredicateCount.get(p) - 1)/(totalIndegreeNodes - 1);
					}
					second.close();
					//outdegree
					ifCurrentPredicateTriplet = "Match (o) -[r:"+ p + "]-> ()\n" +
							"WHERE id(o) = $object and r is not null and id(r) <> $predicate\n" +
							"Return id(o) as o";
					second = db.beginTx();
					isCurrentPredicate = second.execute(ifCurrentPredicateTriplet,
							Map.of("object", object, "subject", subject,  "predicate", predicate));

					double avgOutDegreePrime = 0.0;
					if(isCurrentPredicate.hasNext()){
						avgOutDegreePrime = (originalPredicateCount.get(p) - 1)/totalOutDegreeNodes;
					}else{
						avgOutDegreePrime = (originalPredicateCount.get(p) - 1)/(totalOutDegreeNodes - 1);
					}
					second.close();
					//tolerance condition
					;if(Math.abs(avgIndegree - avgInDegreePrime) < tolerance &&
								Math.abs(avgOutDegree - avgOutDegreePrime) < tolerance){
							String updateSplit = "Match (s) -[r:"+ p + "]-> (o)\n" +
									"WHERE id(s) = $subject and id(o) = $object\n" +
									"SET r.split = $testOrValidate\n" +
									"Return r.split as r";
							second = db.beginTx();
							Integer ifUpdated = -1;
						if(flag){
							ifUpdated = Integer.parseInt(second.execute(updateSplit,
									Map.of("testOrValidate", 1, "subject", subject, "object", object)).next().get("r").toString());
						flag = false;
						} else{
							ifUpdated = Integer.parseInt(second.execute(updateSplit,
									Map.of("testOrValidate", 2, "subject", subject, "object", object)).next().get("r").toString());
							flag = true;
						}

//						ifUpdated = Integer.parseInt(second.execute(updateSplit,
//								Map.of("testOrValidate", flag?0:1, "subject", subject, "object", object)).next().get("r").toString());
//						second.execute(updateSplit);
						second.commit();
						second.close();
						//updating the remaining number of edges in train
						current -= 1;
						//update the number of edges of this particular predicate type
						int currentNumberOfEdges = originalPredicateCount.get(p);
						originalPredicateCount.put(p, currentNumberOfEdges - 1);
					}
				}
				inner.close();
				extra.close();
			}
			if(exitCondition){
				System.out.println("At last originalPredicateCount : "+ originalPredicateCount);
				break;
			}
			tx.close();
			System.out.println("current : "+ current);
		}
		
		// TODO: End of your code.
		
		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}

}
