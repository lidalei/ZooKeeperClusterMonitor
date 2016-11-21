package jmeter.writes.impls;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import jmeter.writes.RemoveQueryAbstract;
import smartzkclient.ZkClient;
import zoo.reader.BenchmarkConstants;

public class RemoveQueryImpl extends RemoveQueryAbstract {
	private static final long serialVersionUID = 1L;

	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		SampleResult results = new SampleResult();
		String queryName = this.getAndRemoveQueryId();
		String zkURL = this.getZkURL(context);

		// Connect to zk
		ZkClient zkCli = new ZkClient();
        System.out.println("Connect to zookeeper server" + zkCli.connect(zkURL));

		results.sampleStart();

		// TODO implement remove query
		zkCli.removeZnodeRecursively(BenchmarkConstants.Benchmark_Root_Znode + "/" + queryName);

		results.sampleEnd();
		results.setResponseCodeOK();
		return results;
	}

}
