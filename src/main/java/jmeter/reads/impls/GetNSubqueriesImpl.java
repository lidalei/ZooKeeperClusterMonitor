package jmeter.reads.impls;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import jmeter.reads.GetQueryAbstract;
import smartzkclient.ZkClient;
import zoo.reader.BenchmarkConstants;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;

public class GetNSubqueriesImpl extends GetQueryAbstract {
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
        String queryZnode = BenchmarkConstants.Benchmark_Root_Znode + "/" + queryName;
		List<String> children = zkCli.getChildren(queryZnode, false);
        HashMap<String, Integer> subqueries = new HashMap<>(children.size());
        for(String subqueryName: children) {
            byte[] data = zkCli.getData(queryZnode + "/" + subqueryName, false, null);
            try{
                subqueries.put(subqueryName, Integer.getInteger(new String(data, "UTF-8")));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

		results.sampleEnd();
		results.setResponseCodeOK();

		return results;
	}
}
