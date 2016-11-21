package jmeter.queries;

import zoo.reader.QueryStructure;

public class TestQueryManager {

	public static void main(String[] args) {
		QueryGenerator g1 = new QueryGenerator();
		QueryGenerator g2 = new QueryGenerator();
		QueryRemover r1 = new QueryRemover();
		//QueryRemover r2 = new QueryRemover();

		g1.run();
		g2.run();
		r1.run();
		//r2.start();

		try {
			g1.join();
			g2.join();
			r1.join();
			//r2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Remover nulls: " + r1.nullCounter);
	}
}
class QueryGenerator extends Thread {
	public void run () {
		QueryManager qm = new QueryManager();
		for (int i=0; i<10000; i++) {
			QueryStructure q = qm.generateNewQuery();
			QueryManager.setNewQuery(q.getName());
			//System.out.println("Added" + q.toString());
		}

	}
}
class QueryRemover extends Thread {
	int nullCounter = 0;
	public void run () {
		for (int i=0; i<10000; i++) {
			if (QueryManager.getAndRemoveQuery()==null) {
				nullCounter++;
			}
			//System.out.println("Removed " + q);
		}
	}
}