package jmeter.reads.impls;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import jmeter.reads.GetQueryAbstract;
import smartzkclient.ZkClient;
import zoo.reader.BenchmarkConstants;

import java.util.NoSuchElementException;

public class GetQueryDataImpl extends GetQueryAbstract {
	private static final long serialVersionUID = 1L;
	ZkClient zkCli = null;

	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		SampleResult results = new SampleResult();
		String zkURL = this.getZkURL(context);

		// connect to zk
		if(zkCli == null) {
			zkCli = new ZkClient();
			zkCli.connect(zkURL);
		}

        String queryName = null;

		try{
            queryName = this.getQueryName();
        }
        catch(NoSuchElementException e) {
		    return new SampleResult();
        }

		
		results.sampleStart();

		// TODO implement
		if(zkCli.getData(BenchmarkConstants.Benchmark_Root_Znode + "/" + queryName, false, null) == null) {
		    return new SampleResult();
        }

		results.sampleEnd();
        results.setSuccessful(true);

		return results;
	}
}
