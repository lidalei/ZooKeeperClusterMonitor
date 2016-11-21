package jmeter.writes;

import java.io.Serializable;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import jmeter.queries.QueryManager;

public abstract class RemoveQueryAbstract extends AbstractJavaSamplerClient implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final String ZK_URL = "ZK_URL";

	@Override
	public Arguments getDefaultParameters() {
		Arguments defaultParameters = new Arguments();
		defaultParameters.addArgument(ZK_URL, "localhost:2181");
		return defaultParameters;
	}
	
	public String getZkURL (JavaSamplerContext context) {
		return context.getParameter(ZK_URL);
	}

	public String getAndRemoveQueryId() {
		return QueryManager.getAndRemoveQuery();
	}
	
	public abstract SampleResult runTest(JavaSamplerContext context);

}
