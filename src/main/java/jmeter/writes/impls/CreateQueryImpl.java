package jmeter.writes.impls;

import com.google.gson.Gson;
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
	ZkClient zkCli = null;
	Gson gson = new Gson();

	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		SampleResult results = new SampleResult();
		QueryStructure query = this.generateNewQuery();
		String zkURL = this.getZkURL(context);

        // connect to zk
		if(zkCli == null) {
			zkCli = new ZkClient();
			zkCli.connect(zkURL);
		}

		String jsonString = gson.toJson(query);

		results.sampleStart();

		// TODO implement query creation
        String queryZnode = zkCli.createZnode(BenchmarkConstants.Benchmark_Root_Znode + "/" + query.getName(), jsonString.getBytes(), CreateMode.PERSISTENT);
        if(queryZnode == null) {
            return new SampleResult();
        }
        for(QueryDeployment subquery: query.getSubQueryDeployment()) {
            String subqueryName = subquery.getName();
            int numberOfInstanceManagers = subquery.getNumberOfInstances();
            String path = zkCli.createZnode(queryZnode + "/" + subqueryName, Integer.toString(numberOfInstanceManagers).getBytes(), CreateMode.PERSISTENT);
            if(path == null) {
                return new SampleResult();
            }
        }

		results.sampleEnd();
        results.setSuccessful(true);

		this.setNewQuery(query.getName());
		return results;
	}

}
