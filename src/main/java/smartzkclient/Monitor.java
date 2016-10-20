package smartzkclient;

import org.apache.zookeeper.CreateMode;
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

    // when a child event occurs, compare the changed children with the "current" ones
    // to identify the specific change according to the comparison and the status of instanceManagers,
    // finally update the current instanceManagers and the status.
    private List<String> activeInstanceManagers = null;

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
     * @param appName - String, the name of an application
     * @return boolean
     */
    public boolean startApp(String appName) {
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
        String instanceManagerRootPath = zkCli.createZnode(appRooPath + "/" + instanceManagerRootZnode, instanceManagerRootZnode.getBytes());
        if(instanceManagerRootPath  == null) {
            System.out.println("Create " + instanceManagerRootZnode + " error");
            return false;
        }

        String orchestratorRootPath = zkCli.createZnode(appRooPath + "/" + orchestratorRootZnode, orchestratorRootZnode.getBytes());
        if(orchestratorRootPath  == null) {
            System.out.println("Create " + orchestratorRootZnode + " error");
            return false;
        }


        System.out.println("Start " + appName + " successfully!");
        return true;
    }

    public boolean setWatchOnInstanceManagers(String appName) {

        //TODO
        final String instanceManagerRootPath = "/" + appName + "/" + instanceManagerRootZnode;


        Watcher dataWatch = new Watcher() {
            public void process(WatchedEvent we) {
                // TODO
                System.out.println("InstanceManager data event - state: " + we.getState() + ", type: " + we.getType());
                // re-watch the znode
                zkCli.getData(instanceManagerRootPath, this, null);
            }
        };

        Watcher childWatch = new Watcher() {
            public void process(WatchedEvent we) {
                // TODO
                System.out.println("InstanceManager children event - state: " + we.getState() + ", type: "+ we.getType());
                // re-watch the children change event
                zkCli.getChildren(instanceManagerRootPath, this, null);
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
                }
                catch(UnsupportedEncodingException e) {
                    e.printStackTrace();
                    System.out.println("Unsupported when converting byte[] to String. While getting data of " + instanceManagerRootPath);
                    return false;
                }
            }

        return true;
    }


    public boolean setWatchOnOrchestrator(String appName) {
        final String orchestratorRootPath = "/" + appName + "/" + orchestratorRootZnode;

        Watcher dataWatcher = new Watcher() {
            public void process(WatchedEvent we) {
                // TODO


                System.out.println("Orchestrator event - state: " + we.getState() + ", type: " + we.getType());
                // re-watch the znode
                zkCli.getData(orchestratorRootPath, this, null);
            }
        };


        Watcher childWatcher = new Watcher() {
            public void process(WatchedEvent we) {
                // TODO
                System.out.println("Orchestrator children event - state: " + we.getState() + ", type: "+ we.getType());
                // re-watch the children change event
                zkCli.getChildren(orchestratorRootPath, this, null);
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


}
