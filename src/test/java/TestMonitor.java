/**
 * Created by Sophie on 17/10/2016.
 */

import smartzkclient.InstanceManager;
import smartzkclient.Orchestrator;

public class TestMonitor {

    public static void main(String[] args) {


        try{
            Orchestrator orchestrator1 = new Orchestrator();
            orchestrator1.start();

            Thread.sleep(1000);

            Orchestrator orchestrator2 = new Orchestrator();
            orchestrator2.start();

            orchestrator1.shutDown();

            Thread.sleep(1000);

            orchestrator2.fail();

            Thread.sleep(1000);

            InstanceManager instanceManager1 = new InstanceManager();
            instanceManager1.start();

            Thread.sleep(1000);

            InstanceManager instanceManager2 = new InstanceManager();
            instanceManager2.start();

            Thread.sleep(1000);

            instanceManager1.fail();

            Thread.sleep(2000);

            instanceManager2.shutDown();

            Thread.sleep(2000);

            InstanceManager instanceManager3 = new InstanceManager();
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
