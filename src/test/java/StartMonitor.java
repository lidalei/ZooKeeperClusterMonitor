import smartzkclient.Monitor;

/**
 * Created by Sophie on 12/11/16.
 */
public class StartMonitor {


    public static void main(String[] args) {
        //        String zkHost = "localhost:2181/Assignment1";
        Monitor monitor = new Monitor();
        monitor.startApp();

        monitor.setWatchOnInstanceManagers();

        monitor.setWatchOnOrchestrator();


        try{
            Thread.sleep(3000);

            System.out.println("Active orchestrators: ");
            for(String orch: monitor.listAllOrchestrators()) {
                System.out.println(orch);
            }

            Thread.sleep(2000);

            System.out.println("Active instanceMnagers: ");
            for(String im: monitor.listAllInstanceManagers()) {
                System.out.println(im);
            }


            Thread.sleep(15000);


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
