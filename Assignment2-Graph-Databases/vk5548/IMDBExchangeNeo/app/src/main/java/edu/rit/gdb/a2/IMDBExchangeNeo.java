package edu.rit.gdb.a2;

import java.io.File;
import java.util.*;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.exists;

import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.io.layout.DatabaseLayout;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

public class IMDBExchangeNeo {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String neo4jFolder = args[2];

		System.out.println(new Date() + " -- Started");

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		BatchInserter inserter = BatchInserters.inserter(DatabaseLayout.of(
				Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, new File(neo4jFolder).toPath()).build()));
		
		// TODO Your code here!!!!
		// Different than primary keys in SQL and _ids in MongoDB, the internal ids of the nodes in Neo4j are global, 
		//		that is, there cannot be two nodes with the same internal id. One option is to let Neo4j assign ids
		//		for us; the problem is that relationships between nodes are specified based on internal ids, so we 
		//		will spend a lot of time just resolving ids. You must find a better way to solve id clashing 
		//		efficiently. Hint: You can count the total number of movies and people in the database and, then,
		//		assign unique ids based on that.
		//
		// Recall to keep memory consumption under control. Recall to use simple queries to retrieve the data 
		//		efficiently.
		//
		// Before creating a relationship, the nodes to be connected must exist.
		//
		// Fields that are Decimal128 should be stored as strings in Neo4j.

		//creating the collection People
		MongoCollection<Document> people = db.getCollection("People");
		//Retrieving the documents
		FindIterable<Document> iterPeople = people.find().batchSize(10000);
		Iterator itP = iterPeople.iterator();
		Long index = 1L;
		while (itP.hasNext()) {
			Document doc = (Document) itP.next();
			Long id = new Long(doc.getInteger("_id"));
			String name = doc.getString("name");
			Integer byear = doc.getInteger("byear");
			Integer dyear = doc.getInteger("dyear");

			Label personLabel = Label.label("Person");
			Map<String, Object> hm = new HashMap<>();
			hm.put("id", id);
			hm.put("name", name);
			if(byear != null){
				hm.put("byear", byear);
			}
			if(dyear != null){
				hm.put("dyear", dyear);
			}
			inserter.createNode(id, hm, personLabel);
			index = id;
		}

		//CREATING A HASHMAP FOR GENRES AND ITS NODE_ID
		Map<String, Long> genreLocation = new HashMap<>();

		//creating the collection Movies
		MongoCollection<Document> movie = db.getCollection("Movies");
		//Retrieving the documents
		FindIterable<Document> iterMovie = movie.find().batchSize(10000);
		Iterator itM = iterMovie.iterator();


		while (itM.hasNext()) {
			Document doc = (Document) itM.next();
			Long id = new Long(doc.getInteger("_id") + index);
			String ptitle = doc.getString("ptitle");
			String otitle = doc.getString("otitle");
			Boolean isadult = doc.getBoolean("isadult");
			Integer noofvotes = doc.getInteger("noofvotes");
			Integer year = doc.getInteger("year");
			List<String> genres = (List<String>) doc.get("genres");
			if(genres != null){
				Label genreLabel = Label.label("Genre");
				for(String gen: genres){

						if(genreLocation.containsKey(gen))
							continue;
						Map<String, Object> genName = new HashMap<>();
						genName.put("name", gen);
						Long nodeId = inserter.createNode(genName, genreLabel);
						genreLocation.put(gen, nodeId);


				}
			}

			String rating="";
			if(doc.get("rating") != null){

				 rating = doc.get("rating", Decimal128.class).toString();
//				 if(otitle.equals("Back to the Future") && year == 1985 ){
//					System.out.println("rating -> " + rating);
//				 }
			}
			Integer runtime = doc.getInteger("runtime");

			Label movieLabel = Label.label("Movie");
			Map<String, Object> hm = new HashMap<>();
			hm.put("id", id);
			hm.put("ptitle", ptitle);
			hm.put("otitle", otitle);
			if(isadult != null){
				hm.put("isadult", isadult);
			}
			if(noofvotes != null){
				hm.put("noofvotes", noofvotes);
			}
			if(rating != null){
				hm.put("rating", rating);
			}
			if(runtime != null){
				hm.put("runtime", runtime);
			}
			if(year != null){
				hm.put("year", year);
			}
			inserter.createNode(id, hm, movieLabel);

		}

		//reiterating over the movie for genre relationship : isClassifiedAs
		FindIterable<Document> iterMovieGenre = movie.find().batchSize(10000);
		Iterator itMG = iterMovieGenre.iterator();
		while(itMG.hasNext()){
			Document doc = (Document) itMG.next();
			Long movieID = new Long(doc.getInteger("_id") + index);
			List<String> genres = (List<String>) doc.get("genres");
			if(genres != null){
				for(String gen : genres){
					Long genID;
					if(genreLocation.containsKey(gen)){
						Map<String, Object> extraneous = new HashMap<>();
						genID = genreLocation.get(gen);
						inserter.createRelationship(movieID, genID, RelationshipType.withName("classifiedAs"), extraneous);
					}

				}
			}
		}

		//re-iterating over people to specify relationships with the movie
		//Retrieving the documents
		FindIterable<Document> iterPeopleRel = people.find().batchSize(10000);
		Iterator itPMR = iterPeopleRel.iterator();
		while(itPMR.hasNext()){
			Document doc = (Document) itPMR.next();
			Long peopleID = new Long(doc.getInteger("_id"));
			//acted
			List<Integer> acted = (List<Integer>) doc.get("acted");
			if(acted != null){
				for(Integer id: acted){
					Long movId = new Long(id) + index;
					Map<String, Object> extraneous = new HashMap<>();
					inserter.createRelationship(peopleID, movId, RelationshipType.withName("actsIn"), extraneous);
				}
			}
			//directed
			List<Integer> directed = (List<Integer>) doc.get("directed");
			if(directed != null){
				for(Integer id : directed){
					Long movId = new Long(id) + index;
					Map<String, Object> extraneous = new HashMap<>();
					inserter.createRelationship(peopleID, movId, RelationshipType.withName("directs"), extraneous);
				}
			}
			//knownfor
			List<Integer> knownfor = (List<Integer>) doc.get("knownfor");
			if(knownfor != null){
				for(Integer id : knownfor){
					Long movId = new Long(id) + index;
					Map<String, Object> extraneous = new HashMap<>();
					inserter.createRelationship(peopleID, movId, RelationshipType.withName("isKnownFor"), extraneous);
				}
			}
			//produced
			List<Integer> produced = (List<Integer>) doc.get("produced");
			if(produced != null){
				for(Integer id : produced){
					Long movId = new Long(id) + index;
					Map<String, Object> extraneous = new HashMap<>();
					inserter.createRelationship(peopleID, movId, RelationshipType.withName("produces"), extraneous);
				}
			}
			//written
			List<Integer> written = (List<Integer>) doc.get("written");
			if(written != null){
				for(Integer id : written){
					Long movId = new Long(id) + index;
					Map<String, Object> extraneous = new HashMap<>();
					inserter.createRelationship(peopleID, movId, RelationshipType.withName("writes"), extraneous);
				}
			}
		}
		// TODO End of your code.

		inserter.shutdown();
		client.close();
	}
	
	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

}
