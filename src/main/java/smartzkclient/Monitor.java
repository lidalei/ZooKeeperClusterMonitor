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

//    final String instanceManager = "instanceManagerRoot";
//    final String orchestrator = "orchestratorRoot";

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
        zkCli.connect(zkHost);
        if(zkCli == null) {
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
        String instanceManagerRootPath = zkCli.createZnode(appRooPath + "/" + instanceManager, instanceManager.getBytes());
        if(instanceManagerRootPath  == null) {
            System.out.println("Create " + instanceManager + " error");
            return false;
        }

        String orchestratorRootPath = zkCli.createZnode(appRooPath + "/" + orchestrator, orchestrator.getBytes());
        if(orchestratorRootPath  == null) {
            System.out.println("Create " + orchestrator + " error");
            return false;
        }


        System.out.println("Start " + appName + " successfully!");
        return true;
    }

    public boolean setWatchOnInstanceManagers(String appName) {

        //TODO
        final String instanceManagerRootPath = "/" + appName + "/" + instanceManager;


        Watcher watcher = new Watcher() {
            public void process(WatchedEvent we) {
                System.out.println("InstanceManager data event - state: " + we.getState() + ", type: " + we.getType());
                // re-watch the znode
                zkCli.getData(instanceManagerRootPath, this, null);
            }
        };

        Watcher childWatcher = new Watcher() {
            public void process(WatchedEvent we) {
                System.out.println("InstanceManager children event - state: " + we.getState() + ", type: "+ we.getType());
                // re-watch the children change event
                zkCli.getChildren(instanceManagerRootPath, this, null);
            }
        };

        byte[] data = zkCli.getData(instanceManagerRootPath, watcher, null);

        List<String> children = zkCli.getChildren(instanceManagerRootPath, childWatcher, null);

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
                    System.out.println(instanceManager + " data is " + new String(data, "UTF-8"));
                    System.out.println(instanceManager + " children are " + children.toString());
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

        //TODO
        final String orchestratorRootPath = "/" + appName + "/" + orchestrator;


        Watcher dataWatcher = new Watcher() {
            public void process(WatchedEvent we) {
                System.out.println("Orchestrator event - state: " + we.getState() + ", type: " + we.getType());
                // re-watch the znode
                zkCli.getData(orchestratorRootPath, this, null);
            }
        };


        Watcher childWatcher = new Watcher() {
            public void process(WatchedEvent we) {
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
                System.out.println(orchestrator + " data is " + new String(data, "UTF-8"));
                System.out.println(orchestrator + " children are " + children.toString());
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
