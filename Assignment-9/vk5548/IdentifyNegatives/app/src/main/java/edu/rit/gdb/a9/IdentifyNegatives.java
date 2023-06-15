package edu.rit.gdb.a9;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class IdentifyNegatives {
	
	private enum SPLIT {TRAIN, VALID, TEST};
	private enum NEG_TYPE {CWA, LCWA, NSE, SE};
	
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final SPLIT split = SPLIT.values()[Integer.valueOf(args[1])]; // Either VALID or TEST.
		final String negatives = args[2];
		final String outputFile = args[3];
		
		System.out.println(new Date() + " -- Started");
//		System.out.println("neo4jFolder: "+ neo4jFolder);
//		System.out.println("split: "+ split);
//		System.out.println("negatives: "+ negatives);
//		System.out.println("outputFile: "+ outputFile);
		
		// Parse negatives.
		List<Long> subjects = new ArrayList<>(), objects = new ArrayList<>();
		List<String> predicates = new ArrayList<>();
		if (negatives.length() > 0)
			for (String neg : negatives.split(";")) {
				String[] spo = neg.split(",");
				subjects.add(Long.valueOf(spo[0]));
				objects.add(Long.valueOf(spo[2]));
				predicates.add(spo[1]);
			}
		
		// Result.
		List<NEG_TYPE> types = new ArrayList<>();
		
		File neo4jDB = new File(neo4jFolder);
		
		DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4jDB.toPath()).
				setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
				setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
		GraphDatabaseService db = service.database("neo4j");

		for (int i = 0; i < subjects.size(); i++) {
			long s = subjects.get(i), o = objects.get(i);
			String p = predicates.get(i);

			// Get split num val
			int splitNum = split.ordinal();

//			if (i==5){
//				System.out.println("hi");
//			}


			// TODO Identify the (s, p, o) negative. You must consider the current split into account. There are four types: CWA, LCWA, NSE and SE.
			//	Once you have identified the type, add it to the 'types' list.

			// Check if LCWA

			if (checkLCWA(db, s, p, o, splitNum)){
				// Check if SE
				if (checkSE(db, s, p, o, splitNum)){
					types.add(NEG_TYPE.SE);
				}
				// Else check if NSE
				else if (checkNSE(db, s, p, o, splitNum)){
					types.add(NEG_TYPE.NSE);
				}
				// Else add LCWA to the list
				else{
					types.add(NEG_TYPE.LCWA);
				}
			}
			// Else add CWA to the list
			else{
				types.add(NEG_TYPE.CWA);
			}
		}

		service.shutdown();

//		// Debug
//		System.out.println(types);

		// Print results!
		PrintWriter writer = new PrintWriter(new File(outputFile));
		writer.println(types);
		writer.close();
		
		System.out.println(new Date() + " -- Done");
	}

	// Check if neg is LCWA
	private static boolean checkLCWA(GraphDatabaseService db, long s, String p, long o, int splitNum) {

		// Return true if s'-p'-o exists such that o <> o'
		Transaction tx1 = db.beginTx();
		String query1 = "MATCH (s)-[p]->(o) \n" +
				"WHERE p.split <= $split and type(p) = $p1 \n" +
				"AND id(s) = $s1 AND id(o) <> $o1 and id(o) <> $s1\n" +
				"RETURN id(o) AS id \n" ;
//				+
//				"LIMIT 1";
		Result result1 = tx1.execute(query1, Map.of("p1", p, "split", splitNum, "s1", s, "o1", o));
		if (result1.hasNext()){
			return true;
		}
		tx1.close();
		result1.close();

		// Return true if s-p'-o' exists such that s <> s'
		Transaction tx2 = db.beginTx();
		String query2 = "MATCH (s)-[p]->(o) \n" +
				"WHERE p.split <= $split and type(p) = $p1\n" +
				"AND id(s) <> $s1 and id(s) <> $o1 AND id(o) = $o1 \n" +
				"RETURN id(s) AS id \n";
//		+ "LIMIT 1";
		Result result2 = tx2.execute(query2, Map.of("p1", p, "split", splitNum, "s1", s, "o1", o));
		if (result2.hasNext()){
			System.out.println("LCWA " +result2.next().get("id").toString());
			return true;
		}
		tx2.close();
		result2.close();

		// If neither of above exists then it is not LCWA
		return false;
	}

	// Check if neg is SE
	private static boolean checkSE(GraphDatabaseService db, long s, String p, long o, int splitNum) {
		// Return true if s'-p'-_ exists such that o' E Objects (p', Gx)
		// Check if s'-p'-_ exists
		Transaction tx1 = db.beginTx();
		String query1 = "MATCH (s)-[p]->() \n" +
				"WHERE p.split <= $split and type(p) = $p \n" +
				"AND id(s) = $s \n" +
				"RETURN id(s) AS id \n" +
				"LIMIT 1";
		Result result1 = tx1.execute(query1, Map.of("p", p, "split", splitNum, "s", s));

		// Check if o' E Objects (p', Gx)
		Transaction tx2 = db.beginTx();
		String query2 = "MATCH ()-[p]->(o)\n"+
				"WHERE p.split <= $split \n" +
				"AND id(o) = $o \n" +
				"RETURN id(o) AS id \n" +
				"LIMIT 1";
		Result result2 = tx2.execute(query2, Map.of("p", p, "split", splitNum,"o", o));

		// Return true if both conditions are satisfied
		if (result1.hasNext() && result2.hasNext()){
			return true;
		}
		tx1.close();
		tx2.close();
		result1.close();
		result2.close();

		// Return true if _-p'-o' exists such that s' E Objects (p', Gx)
		// Check if _-p'-o' exists
		Transaction tx3 = db.beginTx();
		String query3 = "MATCH ()-[p]->(o) \n" +
				"WHERE p.split <= $split and type(p) = $p \n" +
				"AND id(o) = $o \n" +
				"RETURN id(o) AS id \n" +
				"LIMIT 1";
		Result result3 = tx3.execute(query3, Map.of("p", p, "split", splitNum, "o", o));

		// Check if s' E Subjects (p', Gx)
		Transaction tx4 = db.beginTx();
		String query4 = "MATCH (s)-[p]->()\n"+
				"WHERE p.split <= $split and type(p) = $p \n" +
				"AND id(s) = $s \n" +
				"RETURN id(s) AS id \n" +
				"LIMIT 1";
		Result result4 = tx4.execute(query4, Map.of("p", p, "split", splitNum,"s", s));

		// Return true if both conditions are satisfied
		if (result3.hasNext() && result4.hasNext()){
			return true;
		}
		tx3.close();
		tx4.close();
		result3.close();
		result4.close();

		// If neither of above exists then it is not NSE
		return false;
	}

	// Check if neg is NSE
	private static boolean checkNSE(GraphDatabaseService db, long s, String p, long o, int splitNum) {
		// Return true if s'-p'-_ exists such that o' E Subjects (p', Gx)
		// Check if s'-p'-_ exists
		Transaction tx1 = db.beginTx();
		String query1 = "MATCH (s)-[p]->() \n" +
				"WHERE p.split <= $split and type(p) = $p \n" +
				"AND id(s) = $s \n" +
				"RETURN id(s) AS id \n" +
				"LIMIT 1";
		Result result1 = tx1.execute(query1, Map.of("p", p, "split", splitNum, "s", s));

		// Check if o' E Subjects (p', Gx)
		Transaction tx2 = db.beginTx();
		String query2 = "MATCH (o)-[p]->()\n"+
				"WHERE p.split <= $split and type(p) = $p \n" +
				"AND id(o) = $o \n" +
				"RETURN id(o) AS id \n" +
				"LIMIT 1";
		Result result2 = tx2.execute(query2, Map.of("p", p, "split", splitNum,"o", o));

		// Return true if both conditions are satisfied
		if (result1.hasNext() && result2.hasNext()){
			return true;
		}
		tx1.close();
		tx2.close();
		result1.close();
		result2.close();

		// Return true if _-p'-o' exists such that s' E Objects (p', Gx)
		// Check if _-p'-o' exists
		Transaction tx3 = db.beginTx();
		String query3 = "MATCH ()-[p]->(o) \n" +
				"WHERE p.split <= $split and type(p) = $p \n" +
				"AND id(o) = $o \n" +
				"RETURN id(o) AS id \n" +
				"LIMIT 1";
		Result result3 = tx3.execute(query3, Map.of("p", p, "split", splitNum, "o", o));

		// Check if s' E Objects (p', Gx)
		Transaction tx4 = db.beginTx();
		String query4 = "MATCH ()-[p]->(s)\n"+
				"WHERE p.split <= $split and type(p) = $p \n" +
				"AND id(s) = $s \n" +
				"RETURN id(s) AS id \n" +
				"LIMIT 1";
		Result result4 = tx4.execute(query4, Map.of("p", p, "split", splitNum,"s", s));

		// Return true if both conditions are satisfied
		if (result3.hasNext() && result4.hasNext()){
			return true;
		}
		tx3.close();
		tx4.close();
		result3.close();
		result4.close();

		// If neither of above exists then it is not NSE
		return false;
	}


}
