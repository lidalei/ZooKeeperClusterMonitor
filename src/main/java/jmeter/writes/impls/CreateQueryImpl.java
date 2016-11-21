package jmeter.writes.impls;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import jmeter.writes.CreateQueryAbstract;
import org.apache.zookeeper.CreateMode;
import zoo.reader.QueryDeployment;
import zoo.reader.QueryStructure;

import smartzkclient.ZkClient;
import zoo.reader.BenchmarkConstants;

public class CreateQueryImpl extends CreateQueryAbstract {
	private static final long serialVersionUID = 1L;

	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		SampleResult results = new SampleResult();
		QueryStructure query = this.generateNewQuery();
		String zkURL = this.getZkURL(context);

        // connect to zk
		ZkClient zkCli = new ZkClient();
		System.out.println("Connect to zookeeper server" + zkCli.connect(zkURL));


		results.sampleStart();

		// TODO implement query creation
        String queryZnode = zkCli.createZnode(BenchmarkConstants.Benchmark_Root_Znode + "/" + query.getName(), "JSON".getBytes(), CreateMode.PERSISTENT);
        for(QueryDeployment subquery: query.getSubQueryDeployment()) {
            String subqueryName = subquery.getName();
            int numberOfInstanceManagers = subquery.getNumberOfInstances();
            zkCli.createZnode(queryZnode + "/" + subqueryName, Integer.toString(numberOfInstanceManagers).getBytes(), CreateMode.PERSISTENT);
        }

		results.sampleEnd();
		results.setResponseCodeOK();

		this.setNewQuery(query.getName());
		return results;
	}

}
