package jmeter.writes.impls;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import jmeter.writes.RemoveQueryAbstract;
import smartzkclient.ZkClient;
import zoo.reader.BenchmarkConstants;


public class RemoveQueryImpl extends RemoveQueryAbstract {
	private static final long serialVersionUID = 1L;
	ZkClient zkCli = null;

	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		SampleResult results = new SampleResult();
		String zkURL = this.getZkURL(context);

		// Connect to zk
		if(zkCli == null) {
			zkCli = new ZkClient();
			zkCli.connect(zkURL);
		}

        String queryName = this.getAndRemoveQueryId();

		if(queryName == null) {
		    return new SampleResult();
        }

		results.sampleStart();

		// TODO implement remove query
		if(!zkCli.removeZnodeRecursively(BenchmarkConstants.Benchmark_Root_Znode + "/" + queryName)) {
		    return new SampleResult();
        }

		results.sampleEnd();
		results.setSuccessful(true);
		return results;
	}

}
