/**
 * Created by Sophie on 10/10/2016.
 */

import org.apache.zookeeper.CreateMode;
import org.json.JSONObject;
import org.json.JSONTokener;
import smartzkclient.ZkClient;
import java.io.FileInputStream;
import java.io.File;
import java.util.List;
import java.io.FileNotFoundException;

public class TestZkClient {

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {


        ZkClient zkCli = new ZkClient();

        if(!zkCli.connect("localhost")) {
            System.out.println("Connection error!");
            return;
        }
        zkCli.createZnode("/app", "root".getBytes(), CreateMode.PERSISTENT);
        Thread.sleep(1000);
        zkCli.createZnode("/app/1", "child1".getBytes(), CreateMode.PERSISTENT);
        Thread.sleep(1000);
        zkCli.createZnode("/app/2", "child1".getBytes(), CreateMode.PERSISTENT);
        Thread.sleep(1000);
        zkCli.createZnode("/app/123", "Hello".getBytes(), CreateMode.PERSISTENT);
        Thread.sleep(2000);
        zkCli.removeZnode("/app/123");

        System.out.println(zkCli.getChildren("/app", false).toString());
        System.out.println(zkCli.removeZnodeRecursively("/app"));


//        // getChildren
//        List<String> children = zkCli.getChildren("/0/subquery 1/streamArray/stream1/name", false);
//
//        String queryDescriptionFile = "/Users/Sophie/Downloads/Zookeeper-Learn/src/main/resources/query_description.json";
//
//        zkCli.storeQueryInfo("/", queryDescriptionFile);

        zkCli.closeConnection();


    }


}
