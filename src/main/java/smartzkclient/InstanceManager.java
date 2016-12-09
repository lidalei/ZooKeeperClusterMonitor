package smartzkclient;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.UnsupportedEncodingException;

/**
 * Created by Sophie on 11/10/2016.
 * The class is used to simulate the behaviours of an InstanceManager
 */

public class InstanceManager implements ApplicationResources {

    private ZkClient zkCli = new ZkClient();
//    private String zkHost = null;
    private String instanceManagerId = null;
    private static int autoInstanceManagerId = 0;
    private String instanceManagerPath = null;
    private String instanceManagerShadowPath = null;

    public InstanceManager(String instanceManagerId) {
        this.instanceManagerId = instanceManagerId;
    }

    /**
     * auto-assign instanceManager id
     */
    public InstanceManager() {
        this("InstanceManager" + Integer.toString(autoInstanceManagerId));
        autoInstanceManagerId ++;
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

        // create a shadow persistent znode and set the flag of the corresponding associative znode
        instanceManagerShadowPath = zkCli.createZnode("/" + appName + "/" + instanceManagerRootShadowZnode + "/" + instanceManagerId,
                INSTANCE_MANAGER_STARTING.getBytes(), CreateMode.PERSISTENT);
        if(instanceManagerShadowPath == null) {
            System.out.println("Start the instanceManager shadow error.");
            return false;
        }

        // create an EPHEMERAL znode
        instanceManagerPath = zkCli.createZnode("/" + appName + "/" + instanceManagerRootZnode + "/" + instanceManagerId,
                instanceManagerId.getBytes(), CreateMode.EPHEMERAL);

        if(instanceManagerPath == null) {
            System.out.println("Start an instanceManager error.");
            return false;
        }


        // put a data change watcher on the persistent znode
        byte[] data = zkCli.getData(instanceManagerShadowPath, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                byte[] imStatus = zkCli.getData(instanceManagerShadowPath, false, null);
                if(imStatus == null) {
                    System.out.println("Get instanceManager, id: " + instanceManagerId + " data error.");
                    return;
                }
                try {
                    String status = new String(imStatus, "UTF-8");
                    if(!status.equals(ApplicationResources.INSTANCE_MANAGER_STARTING_ACK)) {
                        System.out.println("instanceManager, id: " + instanceManagerId + " did not receive response from monitor.");
                    }
                    else {
                        System.out.println("instanceManager, id: " + instanceManagerId + " received response from monitor.");
                    }
                }
                catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    System.out.println("Get instanceManager, id" + instanceManagerId + " data error.");
                }
            }
        }, null);

        if(data == null) {
            System.out.println("Put data change watcher on instanceManager, id" + instanceManagerId + " error.");
        }

        System.out.println("Start instanceManager, id: " + instanceManagerId + " successfully");
        return true;
    }


    /**
     * shut down
     * set a flag on an associative znode and close connection
     */
    public boolean shutDown() {

        // set the flag of the corresponding associative znode
        if(!zkCli.setData(instanceManagerShadowPath, INSTANCE_MANAGER_SHUTDOWN.getBytes())) {
            System.out.println("InstanceManager " + instanceManagerId + " shutdown error. Reason: fail to set the flag.");
            return false;
        }

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
     * fail - shutdown without setting the dat of shadow znode
     *
     */

    public boolean fail() {

        // set the flag of the corresponding associative znode
//        if(!zkCli.setData(instanceManagerShadowPath, INSTANCE_MANAGER_FAIL.getBytes())) {
//            System.out.println("InstanceManager " + instanceManagerId + " shutdown error. Reason: fail to set the flag.");
//            return false;
//        }

        // close connection
        if(!zkCli.closeConnection()) {
            System.out.println("Close connection error, session ID: " + zkCli.getSessionId());
            return false;
        }

        System.out.println("Fail instanceManager, id: " + instanceManagerId + " successfully");
        return true;
    }


}
