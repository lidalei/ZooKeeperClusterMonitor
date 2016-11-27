package smartzkclient;

/**
 * Created by Sophie on 09/10/2016.
 *
 * This is a smart zookeeper client class.
 *
 */

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.List;

public class ZkClient {

    // create an instance for zookeeper class.
    private ZooKeeper zk = null;

    // declare zookeeper instance to access ZooKeeper ensemble
    final CountDownLatch connectedSignal = new CountDownLatch(1);

    /*
    The function to connect the a specific zookeeper server.
    If connect successfully, return true.
    Else return false.
     */
    public boolean connect(String host) {

        // default as localhost
        if(host.equals("") || host == null) host = "localhost:2181";

        try{
            zk = new ZooKeeper(host, Integer.MAX_VALUE, new Watcher() {
                public void process(WatchedEvent we) {
                    if (we.getState() == Event.KeeperState.SyncConnected) {
                        connectedSignal.countDown();
                    }
                }
            });

            connectedSignal.await();
            return true;
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }

        return false;
    }

    // Method to disconnect from zookeeper server
    public boolean closeConnection() {
        try{
            zk.close();
            return true;
        }
        catch(InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    /*
    Method to create znode in zookeeper ensemble. Default, CreateMode.PERSISTENT.
    If created successfully, return the true path of the created znode,
    Else, return null.
    */
    public String createZnode(String path, byte[] data){
        return createZnode(path, data, CreateMode.PERSISTENT);
    }

    /**
     * Method to create znode in zookeeper ensemble
     * If created successfully, return the true path of the created znode, else if exists, set the data only,
     * Else, return null.
     */
    public String createZnode(String path, byte[] data, CreateMode createMode){
        if(znodeExists(path) != null) {
            if(!setData(path, data)) {
                System.out.println("Znode " + path + " already exists. But update data error.");
                return null;
            }
            System.out.println("Znode " + path + " already exists. Update the data it stores successfully!");
            return path;
        }
        try{
            return zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        }
        catch(KeeperException e) {
            e.printStackTrace();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Delete znode
     */

    public boolean removeZnode(String znodePath) {
        try{
            zk.delete(znodePath, znodeExists(znodePath).getVersion());
            return true;
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        catch (KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Remove recursively znode
     */

    public boolean removeZnodeRecursively(String znodePath) {
        try{
            ZKUtil.deleteRecursive(zk, znodePath);
            return true;
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * get session id
     */

    public Long getSessionId() {
        return zk.getSessionId();
    }

    /**
     * Method to update the data in a znode. Similar to getData but without watcher.
     */
    public boolean setData(String path, byte[] data) {
        try{
            zk.setData(path, data, zk.exists(path, false).getVersion());
            return true;
        }
        catch(KeeperException e) {
            e.printStackTrace();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    private Stat znodeExists(String path) {
        try{
            return zk.exists(path, false);
        }
        catch(KeeperException e){
            e.printStackTrace();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public byte[] getData(String path, Watcher watcher, Stat state) {
        return getDataOrElse(path, watcher, state, null);
    }

    private byte[] getDataOrElse(String path, Watcher watcher, Stat state, byte[] defaultValue) {
        Stat stat = znodeExists(path);

        if(stat != null) {
            try {
                return zk.getData(path, watcher, state);
            }
            catch(KeeperException e) {
                e.printStackTrace();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            return defaultValue;
        }
        else {// error or no such znode exists
            System.out.println("Node does not exists or exceptions occurred while getting data. Refer the output for more information.");
        }
        return defaultValue;
    }


    public byte[] getData(String path, boolean watcher, Stat stat) {
        try{
            return zk.getData(path, watcher, stat);
        }
        catch(KeeperException e) {
            e.printStackTrace();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
    Register an instance manager.
    @param instanceManagerId, id of an instance manager.
    @param subqueries, the name of subqueries executed in this instance manager.
     */
    public String registerInstanceManager(String rootPath, String instanceManagerId, String subqueries) {
        return createZnode(rootPath + "/" + instanceManagerId, subqueries.getBytes());
    }


    /**
     * get direct children and set a watch on the event of children change
     * @param path
     * @param watch
     * @return
     */
    public List<String> getChildren(String path, Watcher watch, Stat state) {

        if(znodeExists(path) == null) {
            return null;
        }

        try {
            return zk.getChildren(path, watch, state);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        catch(KeeperException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * List children without setting a watch
     * @param path - String, parent path to all returned children
     * @param isRecursive - boolean, true, list grandchildren, etc.
     * @return children: List<String>
     */

    public List<String> getChildren(String path, boolean isRecursive) {
        if(znodeExists(path) == null) {
            return null;
        }

        if(!isRecursive) { // not recursive, only return direct children
            try {
                return zk.getChildren(path, false);
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            catch(KeeperException e) {
                e.printStackTrace();
            }
            return null;
        }
        else {
            try {
                List<String> directChildren =  zk.getChildren(path, false);
                // children contain directChildren
                List<String> children = directChildren;
                for(String directChild : directChildren) {
                    List<String> grandChildren = zk.getChildren(directChild, false);
                    if(grandChildren != null)
                        children.addAll(grandChildren);
                }
                return children;
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            catch(KeeperException e) {
                e.printStackTrace();
            }

            return null;
        }

    }


    /*
    The function to store cluster information in zookeeper.
     */
//    TODO
    public boolean storeClusterInfo() {


        return true;
    }


    /*
    Store operatorArray information.
     */
    private boolean storeOperator(String operatorRootPath, JSONObject operator) {
        String operatorName = operator.getString("name");
        String operatorType = operator.getString("type");
        if(createZnode(operatorRootPath + "/name", operatorName.getBytes()) == null) return false;
        if(createZnode(operatorRootPath + "/type", operatorType.getBytes()) == null) return false;


        JSONArray inStreamNames = operator.getJSONArray("inStreamNames");
        String operatorInStreamsRootPath = createZnode(operatorRootPath + "/inStreamNames", "inStreamNames".getBytes());
        for(int i = 0; i < inStreamNames.length(); ++i) {
            String inStreamName = inStreamNames.getString(i);
            if(createZnode(operatorInStreamsRootPath + "/operator" + (i + 1), inStreamName.getBytes()) == null) return false;
        }


        JSONArray outStreamNames = operator.getJSONArray("outStreamNames");
        String operatorOutStreamsRootPath = createZnode(operatorRootPath + "/outStreamNames", "outStreamNames".getBytes());
        for(int i = 0; i < outStreamNames.length(); ++i) {
            String outStreamName = inStreamNames.getString(i);
            if(createZnode(operatorOutStreamsRootPath + "/" + i, outStreamName.getBytes()) == null) return false;
        }

        // config
        String configRootPath = createZnode(operatorRootPath + "/config", "config".getBytes());
        JSONObject config = operator.getJSONArray("config").getJSONObject(0);
        String configKey = config.getString("configKey");
        String configValue = config.getString("configValue");

        if(createZnode(configRootPath + "/configKey", configKey.getBytes()) == null) return false;
        if(createZnode(configRootPath + "/configValue", configValue.getBytes()) == null) return false;


        return true;
    }


    /*
    Store streamArray
     */

    private boolean storeStreamArray(String streamArrayRootPath, JSONArray streamArray){

        for(int i = 0; i < streamArray.length(); ++i) {
            JSONObject streamObj = streamArray.getJSONObject(i);
            String streamName = streamObj.getString("name");
            String streamType = streamObj.getString("type");
            // TODO, several schemas instead of merging them into one
            String streamSchema = streamObj.getString("schema");
            String streamConfig = String.valueOf(streamObj.get("config"));

            String streamIRootPath = createZnode(streamArrayRootPath + "/stream" + (i + 1), ("stream" + (i + 1)).getBytes());
            if(streamIRootPath == null) {
                return false;
            }

            if(createZnode(streamIRootPath + "/name", streamName.getBytes()) == null) return false;
            if(createZnode(streamIRootPath + "/type", streamType.getBytes()) == null) return false;
            if(createZnode(streamIRootPath + "/schema", streamSchema.getBytes()) == null) return false;
            if(createZnode(streamIRootPath + "/config", streamConfig.getBytes()) == null) return false;

        }

        return true;
    }


    /**
     Store continuous queries descriptions information in zookeeper.
     @param rootZnodePath:String - root znode path.
     @param subqueryFile:String - subquery description information.
     */
    public boolean storeQueryInfo(String rootZnodePath, String subqueryFile) {
        try{
            JSONObject subquery = new JSONObject(new JSONTokener(new FileInputStream(new File(subqueryFile))));
            return storeQueryInfo(rootZnodePath, subquery);
        }
        catch(FileNotFoundException e) {
            e.printStackTrace();
            return false;
        }
    }

        /**
        Store continuous queries descriptions information in zookeeper.
         @param rootZnodePath:String - root znode path.
         @param subquery:JSONObject - subquery description information.
         */
    public boolean storeQueryInfo(String rootZnodePath, JSONObject subquery){

        if(rootZnodePath.endsWith("/")) rootZnodePath = rootZnodePath.substring(0, rootZnodePath.length() - 1);

        String id = Integer.toString(subquery.getInt("id"));
        String name = subquery.getString("name");

        String queryZnodePath = rootZnodePath + "/" + id;
        // create znode unsuccessfully
        if(createZnode(queryZnodePath, name.getBytes()) == null) {
            return false;
        }


        JSONArray subqueryArray = subquery.getJSONArray("subqueryArray");

        for(int i = 0; i < subqueryArray.length(); ++i) {
            JSONObject subqueryObj = subqueryArray.getJSONObject(i);

            String subqueryName = subqueryObj.getString("name");
            // store subquery
            String subqueryRootPath = createZnode(queryZnodePath + "/" + subqueryName, "subquery".getBytes());

            // store operatorArray
            String operatorArrayStr = "operatorArray";
            String operatorRootPath = createZnode(subqueryRootPath + "/" + operatorArrayStr, operatorArrayStr.getBytes());
            JSONObject operatorObj = subqueryObj.getJSONArray(operatorArrayStr).getJSONObject(0);

            if(!storeOperator(operatorRootPath, operatorObj)) return false;

            // store streamArray
            String streamArrayStr = "streamArray";
            String streamArrayRootPath = createZnode(subqueryRootPath + "/" + streamArrayStr, streamArrayStr.getBytes());
            JSONArray streamArray = subqueryObj.getJSONArray(streamArrayStr);
            if(!storeStreamArray(streamArrayRootPath, streamArray)) return false;

        }

        return true;
    }



    /*
    Create SubqueryDeploymentJSONArray.
     */
    private boolean createSubqueryDepJSONArray(String queryDepZnodePath, JSONArray subqueryDepJsonArray) {
        for(int i = 0; i < subqueryDepJsonArray.length(); ++i) {
            JSONObject subqueryObj = subqueryDepJsonArray.getJSONObject(i);
            String name = subqueryObj.getString("name");
            String numberOfInstances = subqueryObj.getString("numberOfInstances");
            String stateful = subqueryObj.getString("stateful");
            String routeKey = subqueryObj.getString("routeKey");
            String buckets = subqueryObj.getString("buckets");

            if(createZnode(subqueryDepJsonArray + "/name", name.getBytes()) == null) return false;
            if(createZnode(subqueryDepJsonArray + "/numberOfInstances", numberOfInstances.getBytes()) == null) return false;
            if(createZnode(subqueryDepJsonArray + "/stateful", stateful.getBytes()) == null) return false;
            if(createZnode(subqueryDepJsonArray + "/routeKey", routeKey.getBytes()) == null) return false;
            if(createZnode(subqueryDepJsonArray + "/buckets", buckets.getBytes()) == null) return false;
        }

        return true;
    }

    /*
    Store continuous queries descriptions information in zookeeper.
    @param rootDepZnodePath. The root path to deploy the query.
    @param subqueryDep - subquery deployment information
     */

    public boolean storeQueryDeploymentInfo(String rootDepZnodePath, JSONObject subqueryDep) {
        String name = subqueryDep.getString("name");

        String queryDepZnodePath = null;
        if(znodeExists(rootDepZnodePath + "/" + name) == null) {
            queryDepZnodePath = createZnode(rootDepZnodePath + "/" + name, (name + " Deployment").getBytes());
        }
        else {
            queryDepZnodePath = rootDepZnodePath + "/" + name;
        }
        // create znode unsuccessfully
        if(queryDepZnodePath == null) {
            System.out.println("Create query znode unsuccessfully.");
            return false;
        }

        // Create subqueryDeploymentJSONArray
        JSONArray subqueryDepJSONArray = subqueryDep.getJSONArray("subQueryDeploymentJSONArray");
        if(!createSubqueryDepJSONArray(queryDepZnodePath, subqueryDepJSONArray)) return false;


        return true;
    }




}
