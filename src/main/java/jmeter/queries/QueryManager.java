package jmeter.queries;

import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import zoo.reader.QueryDeployment;
import zoo.reader.QueryStructure;

public class QueryManager {
	private static final Random rand = new Random();
	private static final LinkedList<String> createdQueries = new LinkedList<String>();
	private static final AtomicInteger integerProvider = new AtomicInteger();

	public QueryManager () {
	}
	
	/**
	 * This function takes as input an String with the query name and inserts it at the end of a list.
	 * @param queryName
	 */
	public static synchronized void setNewQuery (String queryName) {
		createdQueries.addLast(queryName);
	}
	
	/**
	 * This query removes and gives the query id of the first query in the list
	 * @return
	 */
	public static synchronized String getAndRemoveQuery() {
		return !createdQueries.isEmpty() ? createdQueries.removeFirst() : null;
	}
	
	/**
	 * Generates a random query and returns the query in the structure QueryStructure
	 * @return
	 */
	public QueryStructure generateNewQuery () {
		int nSubqueries = randomValue1to10();
		QueryDeployment[] instances = new QueryDeployment[nSubqueries];
        for (int i=0; i<nSubqueries; i++) {
        	instances[i] = new QueryDeployment("subquery" + i, randomValue1to10(),
        			false, null, 100);
        }
        return new QueryStructure("query" + integerProvider.getAndIncrement(), instances);
	}
	
	private int randomValue1to10 () {
		return rand.nextInt(9)+1;
	}

	public String getQuery() {
		return createdQueries.getLast();
	}
}
