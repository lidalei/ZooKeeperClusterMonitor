package smartzkclient;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by Sophie on 11/10/2016.
 */
public class Monitor implements ApplicationResources {

    private String zkHost = null;
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

    public Monitor(String zkHost, ZkClient zkCli){
        this.zkHost = zkHost;
        this.zkCli = zkCli;
    }

    public Monitor(String zkHost) {
        this(zkHost, new ZkClient());
    }

    public Monitor() {
        this("localhost");
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
                            System.out.println("A new instanceManager was created: " + im);
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
                                try{
                                    String shutdownOrFail = new String(status, "UTF-8");
                                    if(!shutdownOrFail.equals(INSTANCE_MANAGER_SHUTDOWN)) {
                                        shutdownOrFail = INSTANCE_MANAGER_FAIL;
                                    }
                                    System.out.println("The instanceManager, id: " + activeIm + " " + shutdownOrFail);
                                }
                                catch(UnsupportedEncodingException e) {
                                    e.printStackTrace();
                                    System.out.println("Convert the status of instanceManager, id: " + activeIm + " unsuccessfully!");
                                }
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
                try{
                    System.out.println(instanceManagerRootZnode + " data is " + new String(data, "UTF-8"));
                    System.out.println(instanceManagerRootZnode + " children are " + children.toString());

                    // initialize activeInstanceManagers with the existing children when starting the Monitor
                    activeInstanceManagers = children;
                }
                catch(UnsupportedEncodingException e) {
                    e.printStackTrace();
                    System.out.println("Unsupported when converting byte[] to String. While getting data of " + instanceManagerRootPath);
                    return false;
                }
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
                            System.out.println("A new orchestrator was created: " + orch);
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
                                try{
                                    String shutdownOrFail = new String(status, "UTF-8");
                                    if(!shutdownOrFail.equals(ORCHESTRATOR_SHUTDOWN)) {
                                        shutdownOrFail = ORCHESTRATOR_FAIL;
                                    }
                                    System.out.println("The orchestrator, id: " + activeOrch + " " + shutdownOrFail);
                                }
                                catch(UnsupportedEncodingException e) {
                                    e.printStackTrace();
                                    System.out.println("Convert the status of orchestrator, id: " + activeOrch + " unsuccessfully!");
                                }
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
            try{
                System.out.println(orchestratorRootZnode + " data is " + new String(data, "UTF-8"));
                System.out.println(orchestratorRootZnode + " children are " + children.toString());

                // initialize activeOrchestrators with the existing children when starting the Monitor
                activeOrchestrators = children;
            }
            catch(UnsupportedEncodingException e) {
                e.printStackTrace();
                System.out.println("Unsupported when converting byte[] to String. While getting data of " + orchestratorRootPath);
                return false;
            }
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

    public void setZkHost(String zkHost) { this.zkHost = zkHost; }

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
