package edu.rit.gdb.a6;

import java.io.File;
import java.util.Date;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class ComputeNodeInfluence {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final long initial = Long.valueOf(args[1]);
		final int repeat = Integer.valueOf(args[2]);
		final double beta = Double.valueOf(args[3]);

		System.out.println(new Date() + " -- Started");
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");


		int f = 0;

		for (int i = 0; i < repeat; i++) {
			System.out.println(new Date() + " -- Iteration: " + i);

			// Set the state of all nodes to S
			Transaction txGetAllNodes = db.beginTx();
			String cypherGetAllNodes = "MATCH (n) \n" + "SET n.state = S";
			txGetAllNodes.execute(cypherGetAllNodes);
			txGetAllNodes.commit();
			txGetAllNodes.close();

			// Infect initial node
			Transaction txInfectInitial = db.beginTx();
			String cypherInfectInitial = "MATCH (n) \n" + "WHERE id(n) = $initial \n" + "SET n.state = I";
			Map<String, Object> m = Map.of("initial", initial);
			txGetAllNodes.execute(cypherInfectInitial, m);
			txInfectInitial.commit();
			txInfectInitial.close();


			int fPrime = 0;
			int step = 0;

			boolean areThereAnyInfected = true;

			while (areThereAnyInfected){

				int count = 0;

				// Get all infected nodes
				Transaction txGetInfected = db.beginTx();
				String cypherGetInfected = "MATCH (n) \n" + "WHERE n.state = I \n" + "RETURN id(n) AS id";
				Result infectedNodes = txGetInfected.execute(cypherGetInfected);
				// If there are no infected nodes, stop the while loop
				if (! infectedNodes.hasNext()){
					areThereAnyInfected = false;
					break;
				}
				// Iterate over all the infected nodes
				while (infectedNodes.hasNext()){

					Map<String, Object> infectedN = infectedNodes.next();
					long nodeId = Long.parseLong(infectedN.get("id").toString());

					// Recover the infected node
					Transaction txRecoverInfected = db.beginTx();
					String cypherRecoverInfected = "MATCH (n) \n"
							+ "WHERE id(n) = $nodeId \n"
							+ "SET n.state = R";
					Map<String, Object> m1 = Map.of("$nodeId", nodeId);
					txRecoverInfected.execute(cypherRecoverInfected, m1);
					txRecoverInfected.commit();
					txRecoverInfected.close();

					// Get neighbours of this node
					Transaction txGetNeighbours = db.beginTx();
					String cypherGetNeighbours = "MATCH (n)--(m) \n"
							+ "WHERE id(n) = $nodeId AND m.state = S \n"
							+ "RETURN id(m) AS id";
					Map<String, Object> m2 = Map.of("$nodeId", nodeId);
					Result newInfectedNodes = txGetNeighbours.execute(cypherGetNeighbours, m1);

					// Iterate over all these neighbours to infect them with a random chance
					while(newInfectedNodes.hasNext()){
						Map<String, Object> newInfectedN = newInfectedNodes.next();
						long nodeIdNew = Long.parseLong(newInfectedN.get("id").toString());
						double random = Math.random();
						if (random < beta){
							// Infect this neighbour
							Transaction txInfectNeighbour = db.beginTx();
							String cypherInfectNeighbour = "MATCH (n) \n" + "WHERE id(n) = $nodeIdNew \n" + "SET n.state = I";
							Map<String, Object> m3 = Map.of("nodeIdNew", nodeIdNew);
							txInfectNeighbour.execute(cypherInfectNeighbour, m3);
							txInfectNeighbour.commit();
							txInfectNeighbour.close();
							// Increment the count
							count++;
						}
					}
					txGetNeighbours.commit();
					txGetNeighbours.close();
				}

				fPrime = fPrime + count;
				step++;

				txGetInfected.commit();
				txGetInfected.close();

			}
			f = f + (fPrime/step);
			
			// TODO: Your code here!
			// Infect the initial node. The rest of the nodes must start in state S. In the initial node, property f will store its influence and
			//		property it will store the number of iterations your program has accomplished. At the end of each iteration, you must update
			//		both f and it with the current values.
			// In each step, each infected node will infect its immediate neighbors that are in state S with probability beta. Continue until
			//		there are no more infected nodes.
			
			// TODO All nodes should be in the same state. Do it now or later.
			
			
//			double fP = .0;
//			int steps = 0;
//			long infectedNodes = 1l;

			// TODO Infect initial node.

//			while (infectedNodes > 0) {
//				System.out.println("\t" + new Date() + " -- Infected nodes: " + infectedNodes);
//
//				infectedNodes = 0l;
//				// TODO Infect all neighbors of infecting that are not recovered.
//
//				// TODO Recover infecting nodes.
//
//				// Update fP and steps.
//				fP += infectedNodes * 1.0;
//				steps++;
//			}

			// TODO Update f and it properties of initial node using fP and steps.

			// TODO All nodes should be in the same state. Do it now or later.
		}
		f = f / repeat;


		// TODO: End of your code.
		
		service.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}


}
