package smartzkclient;

import org.apache.zookeeper.CreateMode;

/**
 * Created by Sophie on 11/10/2016.
 * The class is used to simulate the behaviours of an InstanceManager
 */

public class InstanceManager implements ApplicationResources {

    private ZkClient zkCli = new ZkClient();
    private String zkHost = null;
    private String instanceManagerId = null;

    public InstanceManager(String zkHost, String instanceManagerId) {
        this.zkHost = zkHost;
        this.instanceManagerId = instanceManagerId;
    }

    public InstanceManager(String instanceManagerId) {
        this("localhost", instanceManagerId);
    }


    /**
     * initialize - connect to the zookeeper
     */

    private boolean initialize() {
        if(!zkCli.connect(zkHost)) {
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

        // initialize a connection to zookeeper server
        if(!initialize()){
            System.out.println("Start an instanceManager error due to initialize error.");
            return false;
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

        // set the flag of the corresponding associative znode
        // TODO

        // close connection
        if(!zkCli.closeConnection()) {
            System.out.println("Close connection error, session ID: " + zkCli.getSessionId());
            return false;
        }

        System.out.println("Shut instanceManager, id: " + instanceManagerId + " down successfully");
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
