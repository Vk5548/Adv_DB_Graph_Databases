package edu.rit.gdb.a3;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import scala.math.Integral;

public class CypherNaive {
	
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], ///Users/vaidehikalra/RIT/Graph-723/Assignment3/CypherDump
				proteinsFolder = args[1], ///Users/vaidehikalra/Downloads/Proteins/
				listOfTargets = args[2], //backbones_140L.grf,backbones_1C4F.grf
		listOfQueries = args[3]; //mus_musculus_1U34.8.sub.grf
		final boolean induced = Boolean.valueOf(args[4]); //false or true
		final String resultsFolder = args[5]; ///Users/vaidehikalra/RIT/Graph-723/Assignment3/vk5548/CypherNaive/results/
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		Transaction tx = db.beginTx();


//		System.out.println(neo4jFolder + " :neo4jFolder\n" + proteinsFolder + " :proteinsFolder\n" + listOfTargets
//		+ " :listOfTargets\n" + listOfQueries + ":listOfQueries\n" + induced + " :induced\n" +resultsFolder + " :resultsFolder/n");


		Map<String, File> allQueryFiles = new HashMap<>();  //new HashMap which keeps track of all query files


		for (File q : new File(proteinsFolder + "query/").listFiles())
			allQueryFiles.put(q.getName(), q);
		for (String target : listOfTargets.split(","))
			for (String selectedQuery : listOfQueries.split(",")) {
				System.out.println(new Date() + " -- Target: " + target + "; Query: " + selectedQuery);
				String query = new String(Files.readAllBytes(Paths.get(allQueryFiles.get(selectedQuery).toURI())));
//				System.out.println("query : "+ query);
				List<Map<Long, Long>> results = new ArrayList<>();
				try {
					results = matching(tx, query, target, induced);
				} catch (Throwable oops) {
					// Awful issue!
					oops.printStackTrace();
				}
				PrintWriter writer = new PrintWriter(new File(resultsFolder + target + "-" + selectedQuery + "-" + induced + "_Solutions.txt"));
				for (Map<Long, Long> sol : results)
					writer.println(sol);
				writer.close();
				System.out.println(new Date() + " -- Done");
			}
		tx.close();
		service.shutdown();

		System.out.println(new Date() + " -- Done");
	}

	public static List<Map<Long, Long>> matching(Transaction tx, String query, String targetDb, boolean isInduced) throws IOException {
		List<Map<Long, Long>> result = new ArrayList<>();
		
		// Use this variable to build your query.
		StringBuffer cypher = new StringBuffer();
		// Use this variable to store all the query nodes. with labels hopefully
		List<String> nodes = new ArrayList<>();

		// TODO Your code here!!!
		// Transform the subgraph query into a valid Cypher query.
		// The query must return the original id (not the internal, the property id) of each identified node.
		// Recall that to compute induced subgraph matching you must check that, if there is no edge between
		//		two query nodes, there must be no edge between the mapped data nodes.

		//FIRST PROCESS THE STRING QUERY
//		String[] arr = query.split("\n");
		StringReader sr = new StringReader(query);
		BufferedReader br = new BufferedReader(sr);

		Map<String, String> labels = new HashMap<>();
		int numberOfNodes = Integer.parseInt(br.readLine());
		for(int i =1; i <= numberOfNodes; i++){
			String[] nodeWithLabels = br.readLine().split(" ");

			nodes.add(nodeWithLabels[0]);
			labels.put(nodeWithLabels[0], nodeWithLabels[1]);
		}
		//processing neighbors:
		Map<String, List<String>> neighbors = new HashMap<>();
		String relation;
		while((relation=br.readLine()) != null){

			String[] rel = relation.split(" ");
			if(rel.length == 1){
				continue;
			}
			if(rel.length == 2){
				String node1 = rel[0];
				String node2 = rel[1];

					List<String> ls;

					if(neighbors.containsKey(node1)){
						ls = neighbors.get(node1);
						if(!ls.contains(node2))
							ls.add(node2);
					}else{
						ls = new ArrayList<>();
						ls.add(node2);
					}
				neighbors.put(node1, ls);
				if(neighbors.containsKey(node2)){
						ls = neighbors.get(node2);
						if(!ls.contains(node1))
							ls.add(node1);

					}else{

						ls = new ArrayList<>();
						ls.add(node1);
					}
				neighbors.put(node2, ls);


			}
		}
		//converting into cypher:
		//constructing MATCH part
		cypher.append("MATCH ");
		StringBuilder returnStatement = new StringBuilder(" RETURN ");
		for(String node: neighbors.keySet()){
			if(returnStatement.indexOf("u"+node +".id,")== -1)
				returnStatement.append("u"+node +".id,");

			//get labels of node
			String label = labels.get(node);
			//get all the neighbors of node
			List<String> connections = neighbors.get(node);
			for(String con:connections){
				//get the label of the con
				String otherLabel = labels.get(con);
				if(node.compareTo(con) < 1){
					cypher.append("(u" +node+ ":`"+targetDb + "`:"+ label + ") -[:`" +targetDb+"`]- " +
							"(u" +con + ":`" + targetDb + "`:" + otherLabel + "),");
				}

					if(returnStatement.indexOf("u"+con +".id,")== -1)
						returnStatement.append("u"+con +".id,");



			}
		}

		cypher.deleteCharAt(cypher.lastIndexOf(","));
		returnStatement.deleteCharAt(returnStatement.lastIndexOf(","));
		//check if induced or not
		if(isInduced){
			cypher.append(" WHERE ");
			//adding where part of induced query
			for(String node: neighbors.keySet()){
//				String label = labels.get(node);
				List<String> connections = neighbors.get(node);
				for(String ifNeighbor : nodes){
					if(!node.equals(ifNeighbor) && node.compareTo(ifNeighbor) < 0) {
						if (!connections.contains(ifNeighbor)) {
//							String otherLabel = labels.get(ifNeighbor);
							cypher.append(" NOT (u" + node +  ") -[:`" + targetDb + "`]- (u"+ifNeighbor
									+  ") AND ");
						}
					}
				}
			}
			cypher.delete(cypher.length()-5, cypher.length());
		}

//		cypher.delete(cypher.lastIndexOf("AND"),cypher.length() - 1);
		cypher.append(returnStatement);
		int i = 0, j=0;
		// TODO End of your code.
		
		Result r = tx.execute(cypher.toString());
		while(r.hasNext()) {

			Map<String, Object> fromCypher = r.next();
			System.out.println(fromCypher.toString() + " :please be something");
			Map<Long, Long> sol = new HashMap<>();
			for (String id : nodes)
				sol.put(Long.valueOf(id), ((Number) fromCypher.get(resolveNode(id))).longValue());
			result.add(sol);
		}
		r.close();
		
		return result;
	}
	
	// Resolve the node in the query.
	private static String resolveNode(String id) {
		// TODO Your code here!
		// The result of this function should be the column name of the node with id 'id' in the query. For instance,
		//		if you have created a query such as "MATCH (u1)--(u2)..." the result of this function should be how to 
		//		access the original id of the nodes that match with u1 and u2, i.e., u1.id and u2.id.

		return "u"+id+".id";
	}
	

	
}
