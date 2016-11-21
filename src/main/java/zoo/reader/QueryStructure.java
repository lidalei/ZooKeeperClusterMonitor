package zoo.reader;

import java.util.Arrays;

public class QueryStructure {
	private String name;
	private QueryDeployment[] subQueryDeployment;
	
	public QueryStructure(String name, QueryDeployment[] subQueryDeployment) {
		this.name = name;
		this.subQueryDeployment = subQueryDeployment;
	}
	public QueryDeployment[] getSubQueryDeployment() {
		return subQueryDeployment;
	}
	public void setSubQueryDeployment(QueryDeployment[] subQueryDeployment) {
		this.subQueryDeployment = subQueryDeployment;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	@Override
	public String toString() {
		return "QueryStructure [name=" + name + ", subQueryDeployment(len=" + subQueryDeployment.length + ")=" + Arrays.toString(subQueryDeployment) + "]";
	}
	
}