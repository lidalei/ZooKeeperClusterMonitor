package jmeter.reads.impls;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import jmeter.reads.GetQueryAbstract;
import smartzkclient.ZkClient;
import zoo.reader.BenchmarkConstants;

public class GetQueryDataImpl extends GetQueryAbstract {
	private static final long serialVersionUID = 1L;

	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		SampleResult results = new SampleResult();
		String zkURL = this.getZkURL(context);

		// connect to zk
		ZkClient zkCli = new ZkClient();
		zkCli.connect(zkURL);

		String queryName = this.getQueryName();
		
		results.sampleStart();

		// TODO implement
		zkCli.getData(BenchmarkConstants.Benchmark_Root_Znode + "/" + queryName, false, null);

		results.sampleEnd();
		results.setResponseCodeOK();

		return results;
	}
}
