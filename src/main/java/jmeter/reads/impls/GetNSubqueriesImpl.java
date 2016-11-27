package jmeter.reads.impls;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import jmeter.reads.GetQueryAbstract;
import smartzkclient.ZkClient;
import zoo.reader.BenchmarkConstants;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

public class GetNSubqueriesImpl extends GetQueryAbstract {
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
        catch (NoSuchElementException e) {
		    return new SampleResult();
        }

        String queryZnode = BenchmarkConstants.Benchmark_Root_Znode + "/" + queryName;
		
		results.sampleStart();

		// TODO implement
        // get query description data
        byte[] queryData = zkCli.getData(queryZnode, false, null);
        if(queryData == null) {
            return new SampleResult();
        }
        // get children
		List<String> children = zkCli.getChildren(queryZnode, false);
		if(children == null) {
			return new SampleResult();
		}
		// get children data
        HashMap<String, Integer> subqueries = new HashMap<>(children.size()*4/3);
        for(String subqueryName: children) {
            byte[] data = zkCli.getData(queryZnode + "/" + subqueryName, false, null);
			if(data == null) {
				return new SampleResult();
			}

            try{
                subqueries.put(subqueryName, Integer.getInteger(new String(data, "UTF-8")));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

		results.sampleEnd();
		results.setSuccessful(true);

		return results;
	}
}
