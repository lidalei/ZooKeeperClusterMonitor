package zoo.reader;

import java.util.Arrays;

public class QueryDeployment {
	private String name;
	private int numberOfInstances;
	private boolean stateful;
	private String routeKey;
	private int buckets;
	private String[] assignedTo;
	
	public QueryDeployment(String name, int numberOfInstances, boolean stateful, String routeKey, int buckets) {
		super();
		this.name = name;
		this.numberOfInstances = numberOfInstances;
		this.stateful = stateful;
		this.routeKey = routeKey;
		this.buckets = buckets;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getNumberOfInstances() {
		return numberOfInstances;
	}
	public void setNumberOfInstances(int numberOfInstances) {
		this.numberOfInstances = numberOfInstances;
	}
	public boolean isStateful() {
		return stateful;
	}
	public void setStateful(boolean stateful) {
		this.stateful = stateful;
	}
	public String getRouteKey() {
		return routeKey;
	}
	public void setRouteKey(String routeKey) {
		this.routeKey = routeKey;
	}
	public int getBuckets() {
		return buckets;
	}
	public void setBuckets(int buckets) {
		this.buckets = buckets;
	}
	public void setAssignedTo(String[] ims) {
		if (ims.length != numberOfInstances) {
			throw new RuntimeException("The ims assigned is " + ims.length + " and it should be " + numberOfInstances);
		}
		this.assignedTo = ims;
	}
	public String[] getAssignedTo() {
		return assignedTo;
	}

	@Override
	public String toString() {
		return "QueryDeployment [name=" + name + ", numberOfInstances=" + numberOfInstances + ", stateful=" + stateful
				+ ", routeKey=" + routeKey + ", buckets=" + buckets + ", assignedTo=" + Arrays.toString(assignedTo)
				+ "]";
	}
}