package com.joomtu.neo4j;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.rest.graphdb.RestGraphDatabase;

public class Service {

	private GraphDatabaseService restGraphDb;
	private String HOSTNAME = "localhost";
	private int PORT = 7473;
	private String SERVER_ROOT_URI = "http://" + HOSTNAME + ":" + PORT
			+ "/db/data/";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Service test = new Service();
		test.restGraphDb = new RestGraphDatabase(test.SERVER_ROOT_URI);

		test.restGraphDb.shutdown();
	}

	protected Relationship relationship() {
		Iterator<Relationship> it = node().getRelationships(Direction.OUTGOING)
				.iterator();
		if (it.hasNext())
			return it.next();
		return node().createRelationshipTo(restGraphDb.createNode(), Type.TEST);
	}

	public enum Type implements RelationshipType {
		TEST
	}

	protected Node node() {
		return restGraphDb.getReferenceNode();
	}

	protected GraphDatabaseService getRestGraphDb() {
		return restGraphDb;
	}

}