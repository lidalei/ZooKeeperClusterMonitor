/**
 * Created by Sophie on 17/10/2016.
 */

import smartzkclient.InstanceManager;
import smartzkclient.Monitor;

public class TestMonitor {

    public static void main(String[] args) {

        String zkHost = "localhost:2181/Assignment1";
        Monitor monitor = new Monitor(zkHost);
        monitor.startApp();

        monitor.setWatchOnInstanceManagers();

        monitor.setWatchOnOrchestrator();


        InstanceManager instanceManager1 = new InstanceManager(zkHost, "im1");
        instanceManager1.start();

        try{
            Thread.sleep(1000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }

        InstanceManager instanceManager2 = new InstanceManager(zkHost, "im2");
        instanceManager2.start();

        try{
            Thread.sleep(1000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }

        instanceManager1.fail();

        try{
            Thread.sleep(1000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        
        instanceManager2.shutDown();

//        instanceManager2.fail();

//        while (true) {}

    }

}
