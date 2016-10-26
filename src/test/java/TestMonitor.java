/**
 * Created by Sophie on 17/10/2016.
 */

import smartzkclient.InstanceManager;
import smartzkclient.Monitor;
import smartzkclient.Orchestrator;

public class TestMonitor {

    public static void main(String[] args) {

        String zkHost = "localhost:2181/Assignment1";
        Monitor monitor = new Monitor(zkHost);
        monitor.startApp();

        monitor.setWatchOnInstanceManagers();

        monitor.setWatchOnOrchestrator();


        try{
            Orchestrator orchestrator1 = new Orchestrator(zkHost, "orch1");
            orchestrator1.start();

            Thread.sleep(1000);

            Orchestrator orchestrator2 = new Orchestrator(zkHost, "orch2");
            orchestrator2.start();

            orchestrator1.shutDown();

            Thread.sleep(1000);

            System.out.println("Active orchestrators: ");
            for(String orch: monitor.listAllOrchestrators()) {
                System.out.println(orch);
            }

            orchestrator2.fail();

            Thread.sleep(1000);

            InstanceManager instanceManager1 = new InstanceManager(zkHost, "im1");
            instanceManager1.start();

            Thread.sleep(1000);

            InstanceManager instanceManager2 = new InstanceManager(zkHost, "im2");
            instanceManager2.start();

            Thread.sleep(1000);


            System.out.println("Active instanceMnagers: ");
            for(String im: monitor.listAllInstanceManagers()) {
                System.out.println(im);
            }

            instanceManager1.fail();

            Thread.sleep(2000);

            instanceManager2.shutDown();

            Thread.sleep(2000);

            InstanceManager instanceManager3 = new InstanceManager(zkHost, "im3");
            instanceManager3.start();

            Thread.sleep(2000);

            instanceManager3.fail();

            Thread.sleep(2000);

        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }

    }

}
