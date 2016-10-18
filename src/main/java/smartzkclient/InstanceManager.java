package smartzkclient;

import org.apache.zookeeper.CreateMode;

/**
 * Created by Sophie on 11/10/2016.
 * The class is used to simulate the behaviours of an InstanceManager
 */

public class InstanceManager {

    private ZkClient zkCli = new ZkClient();
    private String zkHost = null;
    private String instanceManagerRootZnode = null;
    private String instanceManagerId = null;

    public InstanceManager(String zkHost, String instanceManagerRootZnode, String instanceManagerId) {
        this.zkHost = zkHost;
        this.instanceManagerRootZnode = instanceManagerRootZnode;
        this.instanceManagerId = instanceManagerId;
    }

    public InstanceManager(String instanceManagerRootZnode, String instanceManagerId) {
        this("localhost", instanceManagerRootZnode, instanceManagerId);
    }


    /**
     * initialize - connect to the zookeeper
     */

    public boolean initialize() {
        zkCli.connect(zkHost);
        if(zkCli == null) {
            System.out.println("Initialization error! Cannot connect to " + zkHost + " Zookeeper error.");
            return false;
        }
        return true;
    }


    /**
     * start
     * defined as creating an ephemeral znode in zookeeper and set a flag on an associative znode
     */
    public boolean start() {
        if(zkCli == null) {
            if(!initialize()){
                System.out.println("Start an instanceManager error due to initialize error.");
                return false;
            }
        }

        // create an EPHEMERAL znode
        String instanceManagerPath = zkCli.createZnode("/" + instanceManagerRootZnode + "/" + instanceManagerId,
                instanceManagerId.getBytes(), CreateMode.EPHEMERAL);

        if(instanceManagerPath == null) {
            System.out.println("Start an instanceManager error.");
            return false;
        }

        // set the flag of the corresponding associative znode
        // TODO

        return true;
    }


    /**
     * shut down
     * set a flag on an associative znode and close connection
     */
    public boolean shutDown() {



        return true;
    }


    /**
     *
     * fail
     * set a flag on an associative znode
     *
     */

    public boolean fail() {



        return true;
    }


}
