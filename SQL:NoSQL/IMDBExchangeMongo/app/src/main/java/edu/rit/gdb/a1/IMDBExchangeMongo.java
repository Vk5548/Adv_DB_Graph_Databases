package edu.rit.gdb.a1;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class IMDBExchangeMongo {

	public static void main(String[] args) throws Exception {
		 final String dbURL = args[0];
		 final String user = args[1];
		 final String pwd = args[2];
		 final String mongoDBURL = args[3];
		 final String mongoDBName = args[4];

		System.out.println(new Date() + " -- Started");

		Connection con = DriverManager.getConnection(dbURL, user, pwd);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		//dropping existing collection
		// MOVIE TABLE FROM SQL
		db.getCollection("Movies").drop();
		db.createCollection("Movies");
		MongoCollection<Document> colM = db.getCollection("Movies");
		
		//FETCHING MOVIES FROM SQL DATABASE
		PreparedStatement movieSt = con.prepareStatement("SELECT * from movie");
		movieSt.setFetchSize(/* Batch size */ 50000);
		ResultSet rsMovie = movieSt.executeQuery();
		System.out.print("Movie started " + new Date());

		List<Document> list = new ArrayList<Document>(); //TO STORE SO THAT MOVIES GO IN BATCH
		int count = 0;
		while(rsMovie.next()) {
			Document d = new Document();
			//id
			d.append("_id", rsMovie.getInt("id"));
			
			// ptilte
			String ptitle = rsMovie.getString("ptitle");
			if (ptitle != null) {
				d.append("ptitle", ptitle);
			}
			// otitle
			String otitle = rsMovie.getString("otitle");
			if (otitle != null) {
				d.append("otitle", otitle);
			}
			// adult
			boolean adult = rsMovie.getBoolean("adult");
			d.append("isadult", adult);
			// year
			Integer year = rsMovie.getInt("year");
			if (year != null && year != 0) {
				d.append("year", year);
			}
			// runtime
			Integer runtime = rsMovie.getInt("runtime");
			if (runtime != null && runtime != 0) {
				d.append("runtime", runtime);
			}
			// get rating
			BigDecimal rating = rsMovie.getBigDecimal("rating");
			if (rating != null) {
				d.append("rating", new Decimal128(rating));
			}
//						Decimal128 rating = new Decimal128(rs.getBigDecimal("rating"));

			// get totalVotes
			Integer totalVotes = rsMovie.getInt("totalvotes");
			if (totalVotes != null && totalVotes != 0) {
				d.append("noofvotes", totalVotes);
			}
			list.add(d);
			count++;
			if (count % 20000 == 0) {
				colM.insertMany(list);
				
				list = new ArrayList<>();
				
			}

		}
		colM.insertMany(list);
		
		list = new ArrayList<>();
		
		System.out.println("Movie Done " + new Date());
		rsMovie.close();
		movieSt.close();
		
		//MOVIEGENRE
		PreparedStatement movieGenreSt = con
				.prepareStatement("SELECT * from moviegenre as mg join genre as g" + " on mg.gid = g.id");
		movieGenreSt.setFetchSize(/* Batch size */ 50000);
		ResultSet rsMovieGenre = movieGenreSt.executeQuery();
		while (rsMovieGenre.next()) {
			Document documentToBeUpdated = new Document();
			Integer movieId = rsMovieGenre.getInt("mid");
			documentToBeUpdated.append("_id", movieId);
			String genre = rsMovieGenre.getString("name");
			Document newUpdate = new Document().append("genres", genre);
			Document add = new Document();

			add.append("$push", newUpdate);
			colM.updateOne(documentToBeUpdated, add);
			
		}
		movieGenreSt.close();
		rsMovieGenre.close();
		
		// create people collection
		db.getCollection("People").drop();
		db.createCollection("People");
		MongoCollection<Document> colPeople = db.getCollection("People");
		
		PreparedStatement people = con.prepareStatement("SELECT * from person");
		people.setFetchSize(/* Batch size */ 50000);
		ResultSet rsPeople = people.executeQuery();
		List<Document> listPeople = new ArrayList<Document>();
		count = 0;
		while (rsPeople.next()) {
			Document d = new Document();
			// id
			Integer id = rsPeople.getInt("id");
			d.append("_id", id);
			// name
			String name = rsPeople.getString("name");
			d.append("name", name);
			Integer byear = rsPeople.getInt("byear");
			d.append("byear", byear);
			Integer dyear = rsPeople.getInt("dyear");
			if (dyear != 0) {
				d.append("dyear", dyear);
			}
			listPeople.add(d);
//			colPeople.insertOne(d);
			count++;
			if (count % 20000 == 0) {
				colPeople.insertMany(listPeople);
				
				listPeople = new ArrayList<Document>();
			}

		}
		colPeople.insertMany(listPeople);
		listPeople = new ArrayList<Document>();
		rsPeople.close();
		people.close();

		// ACTOR TABLE
		PreparedStatement actor = con.prepareStatement("SELECT * from actor");
		actor.setFetchSize(/* Batch size */ 50000);
		ResultSet rsActor = actor.executeQuery();
		while(rsActor.next()) {
			Integer mid = rsActor.getInt("mid");
			Integer pid = rsActor.getInt("pid");
			Document peopleDenorm = new Document();
			peopleDenorm.append("_id", pid);
			Document peopleUpdate = new Document().append("acted", mid);
			Document addPeople = new Document();
			addPeople.append("$push", peopleUpdate);
			colPeople.updateOne(peopleDenorm, addPeople);
		}
		
		actor.close();
		rsActor.close();
		
		// Director TABLE
		PreparedStatement director = con.prepareStatement("SELECT * from director");
		director.setFetchSize(/* Batch size */ 50000);
		ResultSet rsDirector = director.executeQuery();
		while (rsDirector.next()) {
			Integer mid = rsDirector.getInt("mid");
			Integer pid = rsDirector.getInt("pid");
			Document peopleDenorm = new Document();
			peopleDenorm.append("_id", pid);
			Document peopleUpdate = new Document().append("directed", mid);
			Document addMovie = new Document();
			Document addPeople = new Document();
			addPeople.append("$push", peopleUpdate);
			colPeople.updateOne(peopleDenorm, addPeople);
		}
		director.close();
		rsDirector.close();

				//Producer
				PreparedStatement producer = con.prepareStatement("SELECT * from producer");
				producer.setFetchSize(/* Batch size */ 50000);
				ResultSet rsProducer = producer.executeQuery();
				while (rsProducer.next()) {
					Integer mid = rsProducer.getInt("mid");
					Integer pid = rsProducer.getInt("pid");
					Document peopleDenorm = new Document();
					peopleDenorm.append("_id", pid);
					Document peopleUpdate = new Document().append("produced", mid);
					Document addMovie = new Document();
					Document addPeople = new Document();
					addPeople.append("$push", peopleUpdate);
					colPeople.updateOne(peopleDenorm, addPeople);
				}
				producer.close();
				rsProducer.close();

				// Writer TABLE
				PreparedStatement writer = con.prepareStatement("SELECT * from writer");
				writer.setFetchSize(/* Batch size */ 50000);
				ResultSet rsWriter = writer.executeQuery();
				while (rsWriter.next()) {
					Integer mid = rsWriter.getInt("mid");
					Integer pid = rsWriter.getInt("pid");
					Document peopleDenorm = new Document();
					peopleDenorm.append("_id", pid);
					Document peopleUpdate = new Document().append("written", mid);
					Document addMovie = new Document();
					Document addPeople = new Document();
					addPeople.append("$push", peopleUpdate);
					colPeople.updateOne(peopleDenorm, addPeople);
				}
				writer.close();
				rsWriter.close();
		//
//				// KnownFor TABLE
				PreparedStatement known = con.prepareStatement("SELECT * from knownfor");
				known.setFetchSize(/* Batch size */ 50000);
				ResultSet rsknownfor = known.executeQuery();
				while (rsknownfor.next()) {
					Integer mid = rsknownfor.getInt("mid");
					Integer pid = rsknownfor.getInt("pid");

					Document peopleDenorm = new Document();

					peopleDenorm.append("_id", pid);

					Document peopleUpdate = new Document().append("knownfor", mid);

					Document addPeople = new Document();

					addPeople.append("$push", peopleUpdate);

					colPeople.updateOne(peopleDenorm, addPeople);
				}
				known.close();
				rsknownfor.close();

		// TODO Your code here!!!!
		// The order of loading is irrelevant since there are no foreign keys in MongoDB.
		// You should load first the "plain" documents, that is, without considering nesting.
		// Once "plain" documents are inserted, you should update the documents that have nested structure.
		// If a value in MySQL is null, the resulting document should not contain any field for that specific value, 
		//		that is, never insert null values in MongoDB.
		
		
		
		// TODO End of your code.

		client.close();
		con.close();
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
