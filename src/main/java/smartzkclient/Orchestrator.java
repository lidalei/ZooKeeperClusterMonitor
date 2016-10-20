package smartzkclient;

import org.apache.zookeeper.CreateMode;

/**
 * Created by Sophie on 11/10/2016.
 */
public class Orchestrator {

    private ZkClient zkCli = new ZkClient();
    private String zkHost = null;
    private String orchestratorRootZnode = null;
    private String orchestratorId = null;

    public Orchestrator(String zkHost, String orchestratorRootZnode, String orchestratorId) {
        this.zkHost = zkHost;
        this.orchestratorRootZnode = orchestratorRootZnode;
        this.orchestratorId = orchestratorId;
    }

    public Orchestrator(String orchestratorRootZnode, String orchestratorId) {
        this("localhost", orchestratorRootZnode, orchestratorId);
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
        String orchestratorPath = zkCli.createZnode("/" + orchestratorRootZnode + "/" + orchestratorId,
                orchestratorId.getBytes(), CreateMode.EPHEMERAL);

        if(orchestratorPath == null) {
            System.out.println("Start an orchestrator error.");
            return false;
        }

        return true;
    }













}
