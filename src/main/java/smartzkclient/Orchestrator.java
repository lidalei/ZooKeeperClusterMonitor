package smartzkclient;

import org.apache.zookeeper.CreateMode;

/**
 * Created by Sophie on 11/10/2016.
 */
public class Orchestrator implements ApplicationResources {

    private ZkClient zkCli = new ZkClient();
    private String zkHost = null;
    private String orchestratorId = null;
    private String orchestratorPath = null;
    private String orchestratorShadowPath = null;

    public Orchestrator(String zkHost, String orchestratorId) {
        this.zkHost = zkHost;
        this.orchestratorId = orchestratorId;
    }

    public Orchestrator(String orchestratorId) {
        this("localhost", orchestratorId);
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
            System.out.println("Start an orchestrator error due to initialize error.");
            return false;
        }

        // create an EPHEMERAL znode
        orchestratorPath = zkCli.createZnode("/" + appName + "/" + orchestratorRootZnode + "/" + orchestratorId,
                orchestratorId.getBytes(), CreateMode.EPHEMERAL);

        if(orchestratorPath == null) {
            System.out.println("Start an orchestrator error.");
            return false;
        }

        // create a shadow persistent znode and set the flag of the corresponding associative znode
        orchestratorShadowPath = zkCli.createZnode("/" + appName + "/" + orchestratorRootShadowZnode + "/" + orchestratorId,
                ORCHESTRATOR_STARTING.getBytes(), CreateMode.PERSISTENT);

        if(orchestratorShadowPath == null) {
            System.out.println("Start the orchestrator shadow error.");
            return false;
        }


        return true;
    }


    /**
     * shut down
     * set a flag on an associative znode and close connection
     */
    public boolean shutDown() {

        // set the flag of the corresponding associative znode
        if(!zkCli.setData(orchestratorShadowPath, ORCHESTRATOR_SHUTDOWN.getBytes())) {
            System.out.println("orchestrator " + orchestratorId + " shutdown error. Reason: fail to set the flag.");
            return false;
        }

        // close connection
        if(!zkCli.closeConnection()) {
            System.out.println("Close connection error, session ID: " + zkCli.getSessionId());
            return false;
        }

        System.out.println("Shut orchestrator, id: " + orchestratorId + " down successfully");
        return true;
    }




    /**
     * shut down
     * set a flag on an associative znode and close connection
     */
    public boolean fail() {

        // close connection
        if(!zkCli.closeConnection()) {
            System.out.println("Close connection error, session ID: " + zkCli.getSessionId());
            return false;
        }

        System.out.println("Fail orchestrator, id: " + orchestratorId + " successfully");
        return true;
    }

}
