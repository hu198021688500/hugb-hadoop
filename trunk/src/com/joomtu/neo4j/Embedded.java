package com.joomtu.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Embedded {
	
	public enum MyRelationshipTypes implements RelationshipType {
		KNOWS
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GraphDatabaseService graphDb = new EmbeddedGraphDatabase(
				"/home/mohadoop/Eclipse/hadoop/neo4j");

		Transaction tx = graphDb.beginTx();
		try {
			Node firstNode = graphDb.createNode();
			Node secondNode = graphDb.createNode();
			Relationship relationship = firstNode.createRelationshipTo(
					secondNode, MyRelationshipTypes.KNOWS);

			firstNode.setProperty("message", "Hello,");
			secondNode.setProperty("message", "world!");
			relationship.setProperty("message", "brave Neo4j ");
			tx.success();

			System.out.print(firstNode.getProperty("message"));
			System.out.print(relationship.getProperty("message"));
			System.out.print(secondNode.getProperty("message"));

		} finally {
			tx.finish();
			graphDb.shutdown();
		}
	}

}
