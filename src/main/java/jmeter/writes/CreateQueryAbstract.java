package jmeter.writes;

import java.io.Serializable;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import jmeter.queries.QueryManager;
import zoo.reader.QueryStructure;

public abstract class CreateQueryAbstract extends AbstractJavaSamplerClient implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final QueryManager queryManager = new QueryManager();
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

	public QueryStructure generateNewQuery () {
		return queryManager.generateNewQuery();
	}
	public void setNewQuery (String queryName) {
		QueryManager.setNewQuery(queryName);
	}
	
	public abstract SampleResult runTest(JavaSamplerContext context);
}
