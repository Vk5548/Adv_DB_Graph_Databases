package edu.rit.gdb.a6;

import java.io.File;
import java.util.*;

import org.apache.commons.collections.map.HashedMap;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;

public class DetectTopKSpreaders {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0],
				neo4jFolderCopy = args[1];
		final int maxIter = Integer.valueOf(args[2]), patience = Integer.valueOf(args[3]);
		final double epsilon = Double.valueOf(args[4]);
		final String folderToPsiGroundTruth ="/Users/vaidehikalra/Downloads/Psi_GroundTruth/Email-Enron_psi.txt";
		
		System.out.println(new Date() + " -- Started");
		
		// TODO: Your code here!!!!
		System.out.println("neo4jFolder : "+ neo4jFolder);
		System.out.println("neo4jFolderCopy : "+ neo4jFolderCopy);
		System.out.println("maxIter : "+ maxIter);
		System.out.println("patience : "+ patience);
		System.out.println("epsilon : "+ epsilon);

//		//grading software inserters
//		// If neo4jMainFolder=X, this is assuming your database is in X/Email-Enron/
		File neo4jDB = new File(neo4jFolder);
		BatchInserter gradeInserter = BatchInserters.inserter(DatabaseLayout.of(
				Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, neo4jDB.toPath()).build()));
		Scanner gt = new Scanner(new File(folderToPsiGroundTruth));
		while (gt.hasNextLine()) {
			String[] idAndPsi = gt.nextLine().split(",");
			gradeInserter.setNodeProperties(Long.valueOf(idAndPsi[0]), Map.of("psi", Long.valueOf(idAndPsi[1])));
		}
		gt.close();
		gradeInserter.shutdown();
		// A node will have its psi property set.
		//done above


		// Compute the eigenvector centrality for each node and store its value in property x. You should also use an auxiliary property xlast.
		
		int totalNodes = 0;
		
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		// Copy the degeneracy core from the original to the auxiliary database.
		// calculate the psiMax

		
		BatchInserter inserter = BatchInserters.inserter(DatabaseLayout.of(
				Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, new File(neo4jFolderCopy).toPath()).build()));
		

		// TODO Get degeneracy core, that is, those nodes such that psi=max(psi). Create induced subgraph using inserter.
		//1. start the transaction
		Transaction txPsiMax = db.beginTx();
		Transaction txGetNeighbors = db.beginTx();
		String psiMax = "MATCH (u) \n" +
				"RETURN MAX(u.psi) as max"; //verified

		String getNodesWithPsiMax="MATCH (u) \n" +

				"WHERE u.psi = $max\n" +
				"RETURN collect(id(u)) as listOfIDs"; //verified

		Long maxPSI = (Long) txPsiMax.execute(psiMax).next().get("max");
		System.out.println("maxPSI :" + maxPSI);
		Map<String, Object> params = new HashMap<>();
		params.put("max", maxPSI);
		Result resultPsiMax = txPsiMax.execute(getNodesWithPsiMax, params);

		List<Long> degeneracyNodes = null;
		System.out.println("size of nodes list : " + degeneracyNodes);//contains the ids of all the nodes
		if(resultPsiMax.hasNext()){
			degeneracyNodes = (List<Long>) resultPsiMax.next().get("listOfIDs");
		}
		System.out.println("size of nodes list : " + degeneracyNodes);
		Set<Long> alreadyExistingNodes = new HashSet<>();
		String getNeighbors = "MATCH (u) -- (x)\n" +
				"WHERE id(u) = $nodeID  AND id(x) IN $degeneracyNodes\n" +
				"RETURN id(x) as id";
		Map<String, Object> getNeighborParams = new HashMap<>();
		int nodesCreated = 0;
		for(Long nodeID: degeneracyNodes){

			//create the current node only if it already does not exist
			if(!alreadyExistingNodes.contains(nodeID)){
				inserter.createNode(nodeID, new HashMap<>());
				//put the node in the set
				alreadyExistingNodes.add(nodeID);
//				System.out.println("node number :" + ++nodesCreated);
			}
			getNeighborParams = new HashMap<>();
			getNeighborParams.put("degeneracyNodes", degeneracyNodes);
			getNeighborParams.put("nodeID", nodeID);


			Result resNeighbors = txGetNeighbors.execute(getNeighbors, getNeighborParams);
			while(resNeighbors.hasNext()){
				Long neighborID = (Long) resNeighbors.next().get("id");
				if(!alreadyExistingNodes.contains(neighborID)){
					//create the neighbor node
					inserter.createNode(neighborID, new HashMap<>());
					//enter the created node into the set
					alreadyExistingNodes.add(neighborID);
//					System.out.println("node number :" + ++nodesCreated);
				}
				if(nodeID < neighborID){
					inserter.createRelationship(nodeID, neighborID, RelationshipType.withName(""), new HashMap<>());
				}
			}
		}
		txGetNeighbors.close();
		txPsiMax.close();
		inserter.shutdown();
		service.shutdown();
		
		
		
		service = new DatabaseManagementServiceBuilder(new File(neo4jFolderCopy).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		db = service.database("neo4j");
		Transaction txCountNodes = db.beginTx();

		
		// TODO Compute eigenvector centrality.
		
		// TODO Initialize x in the database for each node.
		//count the number of nodes
		int numberOfNodes = countNumberOfNodes(txCountNodes);
		txCountNodes.close();
		System.out.println("numberOfNodes :" + numberOfNodes);
		Transaction txSetX = db.beginTx();
		String setX = "MATCH (u) \n" +
				"SET u.x = $val";
		Map<String, Object> setXParams = new HashMap<>();
		double initialX= 1/(double)numberOfNodes;
		System.out.println("initialX " + initialX);
		setXParams.put("val", initialX);
		txSetX.execute(setX, setXParams);
		txSetX.commit();
		txSetX.close();


		// Normalize x.
		normalize(db);
		
		int i = 0;
		double error = .0;
		for (; i < maxIter; i++) {
			// TODO Create xlast as a copy of x.
			Transaction txSetXLast = db.beginTx();
			String setXLast = "MATCH (u) \n" +
					"SET u.xlast = u.x";
			txSetXLast.execute(setXLast);
			txSetXLast.commit();
			txSetXLast.close();

			// TODO Update x for each neighbor using xlast.
			Transaction txSetNeighbor = db.beginTx();
			String setNeighbor = "MATCH (u) --(v) \n" +
					"SET v.x = v.x + u.xlast";
			txSetNeighbor.execute(setNeighbor);
			txSetNeighbor.commit();
			txSetNeighbor.close();
			
			// Normalize again.
			normalize(db);
			
			// TODO Compute error.
			Transaction txError = db.beginTx();
			String calDifference = "MATCH (u) \n" +
					"RETURN u.x as x, u.xlast as xlast";
			Result resDiff = txError.execute(calDifference);
			while(resDiff.hasNext()){
				Map<String, Object> map = resDiff.next();
				error +=  Math.abs((Double)map.get("x") - (Double)map.get("xlast"));
			}
			txError.close();


			
			if (/* TODO Stop? Patience means that we will perform at least such number of iterations. */ i > patience && error < numberOfNodes* epsilon)
				break;
		}
		
		// TODO: End of your code.
		Map<Long, Double> xValues = new HashMap<>();
		Transaction check = db.beginTx();
		Result res = check.execute("MATCH (n) RETURN id(n) AS id, n.x as x ORDER BY n.x DESC, id(n) ASC LIMIT 25" );

		while (res.hasNext()){
			Map<String, Object> mp = res.next();
			xValues.put((long)mp.get("id"), (double)mp.get("x"));
		}

		res.close();
		System.out.println(xValues.toString());
		
		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}
	
	private static void normalize(GraphDatabaseService db) {
		// TODO Normalize x properties.
		double s = 0;
		Transaction txNormalize = db.beginTx();
		String getX = "MATCH (u) \n" +
				"RETURN u.x as x";

		Result resGetX = txNormalize.execute(getX);
		while(resGetX.hasNext()){
			double x = (double) resGetX.next().get("x");
			s += x*x;
		}
		resGetX.close();
		String setX = "MATCH (u) \n" +
				"SET u.x = u.x/$val";
		Map<String, Object> setXParams = new HashMap<>();
		setXParams.put("val", Math.sqrt(s));
		txNormalize.execute(setX, setXParams);
		txNormalize.commit();
		txNormalize.close();
	}

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

}
