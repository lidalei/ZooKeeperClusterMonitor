package smartzkclient;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Created by Sophie on 11/10/2016.
 */
public class Monitor implements ApplicationResources {

//    private String zkHost = null;
    private ZkClient zkCli = null;

    private String instanceManagerRootPath = null;
    private String instanceManagerRootShadowPath = null;

    private String orchestratorRootPath = null;
    private String orchestratorRootShadowPath = null;


    // when a child event occurs, compare the changed children with the "current" ones
    // to identify the specific change according to the comparison and the status of instanceManagers,
    // finally update the current instanceManagers and the status.
    private List<String> activeInstanceManagers = null;
    private List<String> activeOrchestrators = null;

    public Monitor(ZkClient zkCli){
        this.zkCli = zkCli;
    }

    public Monitor() {
        this(new ZkClient());
    }


    /**
     * To monitor the cluster of instanceManager and Orchestrator, it should establishes a connection to Zookeeper
     */

    private boolean initialize() {
        if(!zkCli.connect(zkHost)) {
            System.out.println("Initialization error! Cannot connect to " + zkHost + " Zookeeper error.");
            return false;
        }
        return true;
    }

    /**
     * start the app
     * create the Orchestrator and InstanceManager parent znodes
     * @return boolean
     */
    public boolean startApp() {
        // initialize
        if(initialize() == false) {
            return false;
        }

        String appRooPath = zkCli.createZnode("/" + appName, appName.getBytes(), CreateMode.PERSISTENT);
        if(appRooPath == null) {
            System.out.println("Start " + appName + " error! Create " + appName + " root path error.");
            return false;
        }

        // create instanceManager and Orchestrator root znode
        instanceManagerRootPath = zkCli.createZnode(appRooPath + "/" + instanceManagerRootZnode, instanceManagerRootZnode.getBytes());
        if(instanceManagerRootPath  == null) {
            System.out.println("Create " + instanceManagerRootZnode + " error");
            return false;
        }

        orchestratorRootPath = zkCli.createZnode(appRooPath + "/" + orchestratorRootZnode, orchestratorRootZnode.getBytes());
        if(orchestratorRootPath  == null) {
            System.out.println("Create " + orchestratorRootZnode + " error");
            return false;
        }


        // create instanceManager and Orchestrator root znode shadows
        instanceManagerRootShadowPath = zkCli.createZnode(appRooPath + "/" + instanceManagerRootShadowZnode, instanceManagerRootShadowZnode.getBytes());
        if(instanceManagerRootShadowPath  == null) {
            System.out.println("Create " + instanceManagerRootShadowZnode + " error");
            return false;
        }

        orchestratorRootShadowPath = zkCli.createZnode(appRooPath + "/" + orchestratorRootShadowZnode, orchestratorRootShadowZnode.getBytes());
        if(orchestratorRootShadowPath  == null) {
            System.out.println("Create " + orchestratorRootShadowZnode + " error");
            return false;
        }

        System.out.println("Start " + appName + " successfully!");
        return true;
    }

    public boolean setWatchOnInstanceManagers() {

        Watcher dataWatch = new Watcher() {
            public void process(WatchedEvent we) {
                System.out.println("InstanceManager data event - state: " + we.getState() + ", type: " + we.getType());
                // re-watch the znode
                zkCli.getData(instanceManagerRootPath, this, null);
            }
        };

        Watcher childWatch = new Watcher() {
            public void process(WatchedEvent we) {
                System.out.println("InstanceManager children event - state: " + we.getState() + ", type: "+ we.getType());
                // get the updated InstanceManagers and re-watch the children change event
                List<String> updatedInstanceManagers = zkCli.getChildren(instanceManagerRootPath, this, null);

                // An event occurs when a new instanceManager is created or an instanceManager is failed or shut down,
                // which corresponds to a new child and a deleted child, respectively.
                // Compare updated children with activeInstanceManagers just after last event occurred
                // diff = updatedInstanceManagers - activeInstanceManagers
                if(updatedInstanceManagers.size() > activeInstanceManagers.size()) { // a new instance is created
                    for(String im : updatedInstanceManagers) {
                        boolean isNew = true;
                        for(String activeIm: activeInstanceManagers) {
                            if(activeIm.equals(im)) {
                                isNew = false;
                                break;
                            }
                        }
                        if(isNew) { // the instanceManager is newly created
                            String imLog = "A new instanceManager was created: " + im;
                            // set the shadow znode data as imStartingAck
                            if(!zkCli.setData(instanceManagerRootShadowPath + "/" + im, ApplicationResources.INSTANCE_MANAGER_STARTING_ACK.getBytes())) {
                                imLog += ". But ack sent error.";
                            }
                            else {
                                imLog += ". And ack sent successfully.";
                            }
                            System.out.println(imLog);
                        }
                    }
                }
                else { // an instanceManager is shutdown or failed
                    //TODO: distinguish the status of shutdown and fail
                    for(String activeIm : activeInstanceManagers) {
                        boolean isAlive = false;
                        for(String im: updatedInstanceManagers) {
                            if(im.equals(activeIm)) {
                                isAlive = true;
                                break;
                            }
                        }
                        if(!isAlive) { // the instanceManager is shutdown or failed
                            // check the status of the instanceManger znode shadow
                            byte[] status = zkCli.getData(instanceManagerRootShadowPath + "/" + activeIm, false, null);
                            if(status == null) {
                                System.out.println("Fail to the get the status of instanceManager, id: " + activeIm);
                            }
                            else {
                                String shutdownOrFail = new String(status, StandardCharsets.UTF_8);
                                if(!shutdownOrFail.equals(INSTANCE_MANAGER_SHUTDOWN)) {
                                    shutdownOrFail = INSTANCE_MANAGER_FAIL;
                                    // set the status
                                    if(!zkCli.setData(instanceManagerRootShadowPath + "/" + activeIm, shutdownOrFail.getBytes())) {
                                        System.out.println("Update the status of instanceManager, id: " + activeIm + "error.");
                                    }
                                }
                                System.out.println("The instanceManager, id: " + activeIm + " " + shutdownOrFail);
                            }
                        }
                    }
                }

                activeInstanceManagers = updatedInstanceManagers;
                updatedInstanceManagers = null;
            }
        };

        byte[] data = zkCli.getData(instanceManagerRootPath, dataWatch, null);

        List<String> children = zkCli.getChildren(instanceManagerRootPath, childWatch, null);

        if(data == null) {
            System.out.println("Get " + instanceManagerRootPath + " data Error!");
            return false;
        }
        else if(children == null) {
            System.out.println("Get " + instanceManagerRootPath + " children Error!");
            return false;
        }
        else {
            System.out.println(instanceManagerRootZnode + " data is " + new String(data, StandardCharsets.UTF_8));
            System.out.println(instanceManagerRootZnode + " children are " + children.toString());

            // initialize activeInstanceManagers with the existing children when starting the Monitor
            activeInstanceManagers = children;
        }

        return true;
    }


    public boolean setWatchOnOrchestrator() {
        Watcher dataWatcher = new Watcher() {
            public void process(WatchedEvent we) {
                System.out.println("Orchestrator event - state: " + we.getState() + ", type: " + we.getType());
                // re-watch the znode
                zkCli.getData(orchestratorRootPath, this, null);
            }
        };


        Watcher childWatcher = new Watcher() {
            public void process(WatchedEvent we) {
                // TODO
                System.out.println("Orchestrator children event - state: " + we.getState() + ", type: "+ we.getType());
                // get the updated orchestrator and re-watch the children change event
                List<String> updatedOrchestrators = zkCli.getChildren(orchestratorRootPath, this, null);

                // An event occurs when a new orchestrator is created or an orchestrator is failed or shut down,
                // which corresponds to a new child and a deleted child, respectively.
                // Compare updated children with activeOrchestrators just after last event occurred
                // diff = updatedOrchestrators - activeOrchestrators
                if(updatedOrchestrators.size() > activeOrchestrators.size()) { // a new instance is created
                    for(String orch : updatedOrchestrators) {
                        boolean isNew = true;
                        for(String activeOrch: activeOrchestrators) {
                            if(activeOrch.equals(orch)) {
                                isNew = false;
                                break;
                            }
                        }
                        if(isNew) { // the orchestrator is newly created
                            String orchLog = "A new orchestrator was created: " + orch;
                            // set the shadow znode data as imStartingAck
                            if(!zkCli.setData(orchestratorRootShadowPath + "/" + orch, ApplicationResources.ORCHESTRATOR_STARTING_ACK.getBytes())) {
                                orchLog += ". But ack sent error.";
                            }
                            else {
                                orchLog += ". And ack sent successfully.";
                            }
                            System.out.println(orchLog);
                        }

                    }
                }
                else { // an orchestrator is shutdown or failed
                    //TODO: distinguish the status of shutdown and fail
                    for(String activeOrch : activeOrchestrators) {
                        boolean isAlive = false;
                        for(String orch: updatedOrchestrators) {
                            if(orch.equals(activeOrch)) {
                                isAlive = true;
                                break;
                            }
                        }
                        if(!isAlive) { // the orchestrator is shutdown or failed
                            // check the status of the orchestrator znode shadow
                            byte[] status = zkCli.getData(orchestratorRootShadowPath + "/" + activeOrch, false, null);
                            if(status == null) {
                                System.out.println("Fail to the get the status of orchestrator, id: " + activeOrch);
                            }
                            else {
                                String shutdownOrFail = new String(status, StandardCharsets.UTF_8);
                                if(!shutdownOrFail.equals(ORCHESTRATOR_SHUTDOWN)) {
                                    shutdownOrFail = ORCHESTRATOR_FAIL;
                                    // set the status
                                    if(!zkCli.setData(orchestratorRootShadowPath + "/" + activeOrch, shutdownOrFail.getBytes())) {
                                        System.out.println("Update the status of orchestrator, id: " + activeOrch + " error.");
                                    }
                                }
                                System.out.println("The orchestrator, id: " + activeOrch + " " + shutdownOrFail);
                            }
                        }
                    }
                }

                activeOrchestrators= updatedOrchestrators;
                updatedOrchestrators = null;

            }
        };


        byte[] data = zkCli.getData(orchestratorRootPath, dataWatcher, null);
        List<String> children = zkCli.getChildren(orchestratorRootPath, childWatcher, null);

        if(data == null) {
            System.out.println("Get " + orchestratorRootPath + " data Error!");
            return false;
        }
        else if(children == null){
            System.out.println("Get " + orchestratorRootPath + " children Error!");
            return false;
        }
        else {
            System.out.println(orchestratorRootZnode + " data is " + new String(data, StandardCharsets.UTF_8));
            System.out.println(orchestratorRootZnode + " children are " + children.toString());

            // initialize activeOrchestrators with the existing children when starting the Monitor
            activeOrchestrators = children;
        }

        return true;
    }

    /**
     * Advice Orchestrator
     * @return boolean
     */
    private boolean adviceOrchestrator() {

//        TODO
        return true;
    }

//    public void setZkHost(String zkHost) { this.zkHost = zkHost; }

    public String getZkHost() { return this.zkHost; }


    /**
     * List all instanceManagers.
     */
    public List<String> listAllInstanceManagers() {
        return zkCli.getChildren(instanceManagerRootPath, false);
    }

    /**
     * List all orchestrators.
     */
    public List<String> listAllOrchestrators() {
        return zkCli.getChildren(orchestratorRootPath, false);
    }

}
