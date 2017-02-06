# ZooKeeper Cluster Monitor
This project is to implement a cluster monitor on top of ZooKeeper. It was developed as a course project (Large Scale Systems Project) at Technical University of Madrid.

# Package Description
* smartzkclient is used to create Monitor, InstanceManager and Orchestrator. The ZkClient class provides functions to interact with ZooKeeper Server. These functions handle ZooKeeper exceptions and return appropriate values, such as null and false.
* jmeter and zoo are used to perform Jmeter Benchmark test. The new version is in diegoburgos/ZooKeeperJMeter.

# How to Test
First, start ZooKeeper server.
Second, run StartMonitor.java.
Third, run TestMonitor.java.
The default ZooKeeper host is localhost:2181, which can be changed in smartzkclient.ApplicationResources.java.
