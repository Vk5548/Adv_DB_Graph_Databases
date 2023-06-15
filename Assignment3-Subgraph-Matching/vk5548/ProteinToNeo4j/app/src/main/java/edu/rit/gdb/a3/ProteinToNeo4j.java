package edu.rit.gdb.a3;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.io.layout.DatabaseLayout;

public class ProteinToNeo4j {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final String proteinsFolder = args[1];

		final String ENTITY2ID = "entity2id";
		final String DATASET = "dataset";
		final String RELATION2ID = "relation2id";

		//my variables

		System.out.println(new Date() + " -- Started");

		BatchInserter inserter = BatchInserters.inserter(DatabaseLayout.of(
				Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, new File(neo4jFolder).toPath()).build()));
		File targetFolder = new File(proteinsFolder ); // "target/"
		///Users/vaidehikalra/Downloads/target
		System.out.println(new File(String.valueOf(targetFolder)));
		//read the files
		File[] files = new File(String.valueOf(targetFolder)).listFiles();
		if(files == null) return;
		Map<String, File> filesMap= new HashMap<>();
		for(File file:files){
			String fileName = file.getName();
			if(fileName.contains(ENTITY2ID)){
				filesMap.put(ENTITY2ID, file);
			}
			if(fileName.contains(DATASET)){
				filesMap.put(DATASET, file);
			}
			if(fileName.contains(RELATION2ID)){
				filesMap.put(RELATION2ID, file);
			}
		}
		files = null;
		boolean flagA = false, flagB = false, flagC = false;
//		while(true){
//		for (File file : files) { // will receive 3 files
			//get the file name
			File file = filesMap.get(ENTITY2ID);
			System.out.println("file " +file.getName());
			String fileName = file.getName();
			if(!fileName.contains(".txt")){
				System.out.println("Execution failed-------");
			}
			//new code
			//FIRST IF CONDITION TO BE EXECUTED
			if(fileName.contains(ENTITY2ID)){
				flagA = true;
				FileReader fr = new FileReader(file);

				BufferedReader br=new BufferedReader(fr);
				//first line is the number of nodes
				int numberOfNodes = Integer.parseInt(br.readLine());
				//creating all the nodes
				int i = 0;
				for( i =0; i < numberOfNodes; i++){

					Integer nodeID = Integer.parseInt(br.readLine().split("\t")[1]);

					System.out.println("nodeID : "+ nodeID);
					Map<String, Object> attributes = new HashMap<>();
					inserter.createNode(nodeID, attributes);
				}
				System.out.println("numberOfNodes after: "+ (i - 1));
			}
			//SECOND IF CONDITION TO BE EXECUTED
			file = filesMap.get(RELATION2ID);
			fileName = file.getName();
			Map<Integer, String> relationshipMap = null;
			if(fileName.contains(RELATION2ID)){
				flagB = true;
				relationshipMap = new HashMap<>();
				FileReader fr = new FileReader(file);
				BufferedReader br=new BufferedReader(fr);
				//first line is the number of type of relationships or Predicates
				int numberOfPredicates = Integer.parseInt(br.readLine());
				for(int i =0; i < numberOfPredicates; i++){
					String[] relation = br.readLine().split("\t");
					String relationType = relation[0];
					relationshipMap.put(Integer.parseInt(relation[1]), relation[0]);
				}
				System.out.println("relationshipMap : "+ relationshipMap);
			}
			//THIRD IF CONDITION TO BE EXECUTED
			file = filesMap.get(DATASET);
			fileName = file.getName();
			if(fileName.contains(DATASET)){
				flagC = true;
				FileReader fr = new FileReader(file);
				BufferedReader br=new BufferedReader(fr);
				//all lines are similar
				//2122 517 3
				String line = "";
				int numberORelationships = 0;
				while((line = br.readLine()) != null){
					String[] split = line .split(" ");
					Long nodeIDOne = Long.parseLong(split[0]);
					Long nodeIDTwo = Long.parseLong(split[1]);
					String relationshipType = relationshipMap.get(Integer.parseInt(split[2]));
					inserter.createRelationship(nodeIDOne, nodeIDTwo, RelationshipType.withName(relationshipType), new HashMap<>());
					numberORelationships++;
				}
				System.out.println("Relationships created : " + numberORelationships);
			}


//		}
		inserter.shutdown();

		// TODO Your code here!!!!
		// Read the GRF files, which are nothing more than text files, in the target folder.
		// We will create a single Neo4j database containing all disconnected graphs. Neo4j v4 does not
		//		provide any solution to deal with multiple disconnected graphs. Our solution will consist
		//		of assigning labels to nodes to distinguish where each node belongs to. These labels will
		//		be the file names of the target graphs.
		// We need to store the original node ids provided by the Proteins dataset in order to check
		//		them w.r.t. the ground truth. These node ids will be saved as a property 'id' for each node.
		// We will consider undirected edges; however, the Proteins dataset contains both edges, e.g.,
		//		(0, 1) and (1, 0); you must only create a single edge in any direction.


		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");
		Transaction tx = null;
		tx = db.beginTx();
		String check = "MATCH () -[r]- () \n" +
				"RETURN COUNT(r) as cnt";
		System.out.println("count of nodes: " + tx.execute(check).next().get("cnt"));
		// TODO End of your code.
		tx.close();

	}

}









//			//old code----------------------------------------------------------------------------------
//			Map<Integer, Long> trackIdOfSingleFile = new HashMap<>();
//			FileReader fr = new FileReader(file);
//
//			BufferedReader br=new BufferedReader(fr);  //creates a buffering character input stream
//
//			boolean flag = true;
//			String b = "";
//			long nodes =0;
//			long edges = 0;
//			while(flag){
//				b = br.readLine();
//				if(b.contains("Nodes:")){
//					String[] arr = b.split(" ");
//					for(int i =0; i < arr.length; i++){
//						if(arr[i].contains("Nodes:")){
//							nodes = Integer.parseInt(arr[i+1]);
//						}
//						if(arr[i].contains("Edges:")){
//							edges = Integer.parseInt(arr[i+1]);
//						}
//					}
//					flag = false;
//				}
//			}
//
////			System.out.println("b " +b);
////			Integer totalNodes = Integer.parseInt(b);
//
//			//creating all the nodes
//			for(int i =0; i< nodes; i++){
////				System.out.println("i :" + i);
////				String line = br.readLine();
////				System.out.println(line + " :line");
////				String variableLabel = line.split(" ")[1]; //[0, N]
//				Map<String, Object> attributes = new HashMap<>();
//
////				attributes.put("id", i);
//				long nodeID = Long.valueOf(i);
//				attributes.put("id", nodeID);
//				inserter.createNode(nodeID, attributes);
////				Long nodeID = inserter.createNode(attributes);
////				if(!trackIdOfSingleFile.containsKey(i)){
////					trackIdOfSingleFile.put(i, nodeID);
////				}
//			}
//
//			String relationship;
//			while((relationship = br.readLine()) != null){
//
//
//				if(relationship.contains("FromNodeId")){
//					continue;
//				}else{
//					String[] actualRel = relationship.split("\t");
//					Integer nodeOne = Integer.parseInt(actualRel[0]);
//					Integer nodeTwo = Integer.parseInt(actualRel[1]);
//					if( nodeOne < nodeTwo){  //
//						Map<String, Object> relAttr = new HashMap<>();
//						//get the nodeIDS of node
//						Long nodeIDOne = Long.valueOf(nodeOne);
//						Long nodeIDTwo = Long.valueOf(nodeTwo);
//						inserter.createRelationship(nodeIDOne, nodeIDTwo, RelationshipType.withName(""), relAttr);
////						System.out.println("nodeOne :" + nodeOne + "\nnodeTwo: "+ nodeTwo);
//
//					}
//				}
//
//			}
//

