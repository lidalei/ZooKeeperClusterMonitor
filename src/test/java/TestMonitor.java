/**
 * Created by Sophie on 17/10/2016.
 */

import smartzkclient.Monitor;

public class TestMonitor {

    public static void main(String[] args) {
        String appName = "query";
        Monitor monitor = new Monitor("localhost:2181/Assignment1");
        monitor.startApp(appName);

        monitor.setWatchOnInstanceManagers(appName);

        monitor.setWatchOnOrchestrator(appName);

        while (true) {}

    }

}
