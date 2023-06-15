package edu.rit.gdb.a4;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;

import java.io.File;
import java.util.*;

public class CreateProfiles {

	public static void main(String[] args) {
		final String neo4jFolder = args[0];

		System.out.println(new Date() + " -- Started");
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(new File(neo4jFolder).toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");


//--------------------------------------------------------------------------------------------
		try{
			Transaction transaction = db.beginTx(); //transaction which is just going to get the nodes
			String getMaxNode = "MATCH (u)\n" +
					"WITH max(id(u)) as maxID\n" +
					"RETURN maxID";
			Result result = transaction.execute(getMaxNode);
			Long maxNodeID = null;
			while (result.hasNext()){
				Map<String, Object> maxNodeMap = result.next();
				maxNodeID= (Long) maxNodeMap.get("maxID");
			}
			result.close();
			transaction.close();
//			ResourceIterable<Node> allNodes = transaction.getAllNodes();
//			Iterator<Node> iter = allNodes.iterator();
			List<String> profile = new ArrayList<String>();
			int batchSize = 1000, cnt= 0;

			Transaction tx = db.beginTx(); //transaction which will be used to commit the changes
			for(int i =0; i<=maxNodeID; i++){
//			while(iter.hasNext()){ // for each node
				cnt++;
				Node node = tx.getNodeById(Long.valueOf(i));
				if(node == null){
					continue;
				}
				Long nodeID = node.getId();
				String cypher = "MATCH (u) -- (x)\n " +
						"WHERE id(u) = $id\n" +
						"RETURN labels(x) as labels";
				Map<String,Object> params = new HashMap<>();
				params.put("id", nodeID);
				Result res = tx.execute(cypher, params); //got labels of each neighbour node
				while(res.hasNext()){
					List<String> ls = (List<String>) res.next().get("labels");
					for(String single:ls){ // runs twice for this particular case
						profile.add(single);
					}

				}// all labels are added into the list
				res.close();
				String[] profileArray=  new String[profile.size()];

				for(int j =0; j< profile.size(); j++){
					profileArray[j] = profile.get(j);
				}
//				System.out.println("Size profile: " +profile.size());
				profile= new ArrayList<>();
				Arrays.sort(profileArray); //sorting them lexicographically

				String setP = "MATCH (u)\n" +
						"WHERE id(u) = $id \n" +
						"SET u.profile = $profile \n" +
						"RETURN id(u)";
//				Map<String,Object> updateParams = new HashMap<>();
//				updateParams.put("id", nodeID);
//				updateParams.put("profile", profileArray);
//				tx.execute(setP, updateParams);
				node.setProperty("profile", profileArray);
				profileArray=  new String[profile.size()];
				if(cnt % batchSize == 0){
					System.out.println("committed");
					tx.commit();
					tx.close();
					tx = db.beginTx();

				}
			}
			tx.commit();
			tx.close();
//			tx = db.beginTx();
//			String result= "MATCH (u) WHERE id(u) ="+ Long.valueOf(1)+ " RETURN u";
//			Result rs = tx.execute(result);
//			while(rs.hasNext()){
//				Map<String, Object> val = rs.next();
//
//				System.out.println(val.toString());
//				int x, y, z=0;
//			}
//			tx.close();

		}catch (Exception e){
			System.out.println("exception caused: " + e.getStackTrace() + "\n "+ e.getMessage());
			e.printStackTrace();
		}
		finally {

		}

//--------------------------------------------------------------------------------------------
//		try{
//			Transaction transaction = db.beginTx(); //transaction which is just going to get the nodes
//			ResourceIterable<Node> allNodes = transaction.getAllNodes();
//			Iterator<Node> iter = allNodes.iterator();
//			List<String> profile = new ArrayList<String>();
//			int batchSize = 300, cnt = 0;
//
//			Transaction tx = db.beginTx(); //transaction which will be used to commit the changes
//			while(iter.hasNext()){ // for each node
//				cnt++;
//				Node node = iter.next();
//				Long nodeID = node.getId();
//				String cypher = "MATCH (u) -- (x)\n " +
//						"WHERE id(u) = $id\n" +
//						"RETURN labels(x) as labels";
//				Map<String,Object> params = new HashMap<>();
//				params.put("id", nodeID);
//				Result res = tx.execute(cypher, params); //got labels of each neighbour node
//				while(res.hasNext()){
//					List<String> ls = (List<String>) res.next().get("labels");
//					for(String single:ls){ // runs twice for this particular case
//						profile.add(single);
//					}
//
//				}// all labels are added into the list
//
//				String[] profileArray=  new String[profile.size()];
//
//				for(int i =0; i< profile.size(); i++){
//					profileArray[i] = profile.get(i);
//				}
//				Arrays.sort(profileArray); //sorting them lexicographically
//
//				String setP = "MATCH (u)\n" +
//						"WHERE id(u) = $id \n" +
//						"SET u.profile = $profile \n" +
//						"RETURN id(u)";
//				Map<String,Object> updateParams = new HashMap<>();
//				updateParams.put("id", nodeID);
//				updateParams.put("profile", profileArray);
//				tx.execute(setP, updateParams);
//
//				profileArray=  new String[profile.size()];
//				if(cnt % batchSize == 0){
//					tx.commit();
//					tx.close();
//					tx = db.beginTx();
//
//				}
//			}
//			tx.commit();
//			tx.close();
////			tx = db.beginTx();
////			String result= "MATCH (u) WHERE id(u) ="+ Long.valueOf(1)+ " RETURN u";
////			Result rs = tx.execute(result);
////			while(rs.hasNext()){
////				Map<String, Object> val = rs.next();
////
////				System.out.println(val.toString());
////				int x, y, z=0;
////			}
////			tx.close();
//
//		}catch (Exception e){
//
//		}
//		finally {
//
//		}
//		// TODO Your code here!!!!
		// For each node in the database, create a profile lexicographically sorted based on the node labels of its neighbors.
		// The property must be named 'profile' (no quotation marks).
		// To keep memory under control, you need to deal with small transactions as follows:
		//		Init transaction
		//		cnt := 0
		//		Loop ...
		//			cnt := cnt + 1
		//			...
		//			If cnt % X == 0:
		//				Transaction commit and close
		//				Init transaction
		//			End if
		//		End loop 
		//		Transaction commit and close
		//
		// If you are performing modifications to the database but are not able to see any of them, this tipically means that
		//		there is a dangling transaction. Please, debug your code.

		
		
		// TODO End of your code.
		
		System.out.println(new Date() + " -- Done");

		service.shutdown();
	}

}
