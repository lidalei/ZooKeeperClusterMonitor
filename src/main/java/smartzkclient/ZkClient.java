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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.List;

public class ZkClient {

    // create static instance for zookeeper class.
    private static ZooKeeper zk = null;

    // declare zookeeper instance to access ZooKeeper ensemble
    private ZooKeeper zoo = null;
    final CountDownLatch connectedSignal = new CountDownLatch(1);


    /*
    The function to connect the a specific zookeeper server.
    If connect successfully, return the zookeeper object.
    Else throw exception.
     */
    public ZooKeeper connect(String host) throws IOException,InterruptedException {

        // default as localhost
        if(host == "" || host == null) host = "localhost";

        zoo = new ZooKeeper(host, 5000, new Watcher() {

            public void process(WatchedEvent we) {

                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });

        connectedSignal.await();
        return zoo;
    }

    // Method to disconnect from zookeeper server
    public void closeConnection() throws InterruptedException {
        zoo.close();
    }


    /*
     Method to create znode in zookeeper ensemble
     If created successfully, return the true path of the created znode,
      Else, return null.
      */
    private String createZnode(String path, byte[] data){
        try{
            return zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        catch(KeeperException e) {
            e.printStackTrace();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Stat znode_exists(String path) {
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

    private byte[] getDataOrElse(String path, Watcher watcher, Stat state, byte[] defaultValue) {
        Stat stat = znode_exists(path);

        // error or no such znode exists
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
        else {
            System.out.println("Node does not exists or exceptions occurred while getting data. Refer the output for more information.");
        }
        return defaultValue;
    }


    /*
    Register an instance manager.
    @param instanceManagerId, id of an instance manager.
    @param subqueryName, the name of subqueries executed in this instance manager.
     */
    public String registerInstanceManager(String rootPath, String instanceManagerId, String subqueries) {
        return createZnode(rootPath + "/" + instanceManagerId, subqueries.getBytes());
    }

    /*
    List all instance manager.
    @param rootPath.
     */

    public List<String> listAllInstanceManagers(String rootPath) {
        if(znode_exists(rootPath) == null) {
            return null;
        }

        try {
            return zk.getChildren(rootPath, false);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        catch(KeeperException e) {
            e.printStackTrace();
        }
        return null;
    }

    /*
    List all instance managers where the subquery belonging to query is deployed.
     */
    public List<String> listAllInstanceManagersWithQuery(String queryRootPath) {
        //TODO

        return null;
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
            if(createZnode(operatorInStreamsRootPath + "/" + i, inStreamName.getBytes()) == null) return false;
        }


        String outStreamNames = operator.getJSONArray("outStreamNames").getString(0);
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
            String streamConfig = streamObj.getString("config");

            if(createZnode(streamArrayRootPath + "/name", streamName.getBytes()) == null) return false;
            if(createZnode(streamArrayRootPath + "/type", streamType.getBytes()) == null) return false;
            if(createZnode(streamArrayRootPath + "/schema", streamSchema.getBytes()) == null) return false;
            if(createZnode(streamArrayRootPath + "/config", streamConfig.getBytes()) == null) return false;

        }

        return true;
    }

    /*
    Store continuous queries descriptions information in zookeeper.
    @param subquery - subquery description information.
     */
    public boolean storeQueryInfo(String rootZnodePath, JSONObject subquery){
        String id = subquery.getString("id");
        String name = subquery.getString("name");

        String queryZnodePath = null;
        if(znode_exists(rootZnodePath + "/" + id) == null) {
            queryZnodePath = createZnode(rootZnodePath + "/" + id, name.getBytes());
        }
        else {
            queryZnodePath = rootZnodePath + "/" + id;
        }
        // create znode unsuccessfully
        if(queryZnodePath == null) {
            System.out.println("Create query znode unsuccessfully.");
            return false;
        }


        JSONObject subqueryArrayObj = subquery.getJSONArray("subqueryArray").getJSONObject(0);
        String subqueryName = subqueryArrayObj.getString("name");
        // store subqueryArray
        String operatorRootPath = createZnode(queryZnodePath + "/" + subqueryName, "operatorArray".getBytes());

        JSONObject operatorObj = subqueryArrayObj.getJSONArray("operatorArray").getJSONObject(0);

        if(!storeOperator(operatorRootPath, operatorObj)) return false;

        // store streamArray
        JSONArray streamArray = subquery.getJSONArray("streamArray");
        String streamArrayRootPath = createZnode(queryZnodePath + "/streamArray", "streamArray".getBytes());
        if(!storeStreamArray(streamArrayRootPath, streamArray)) return false;

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
        if(znode_exists(rootDepZnodePath + "/" + name) == null) {
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
