package smartzkclient;

/**
 * Created by Sophie on 17/10/2016.
 * This interface contains all agreements between Monitor, InstanceManager and Orchestrator.
 */
public interface ApplicationResources {
    String zkHost = "localhost:2181";
    String appName = "SystemProjectOne";
    String instanceManagerRootZnode = "instanceManagerRoot";
    String orchestratorRootZnode = "orchestratorRoot";
    String instanceManagerRootShadowZnode = "instanceManagerRootShadow";
    String orchestratorRootShadowZnode = "orchestratorRootShadow";

    String INSTANCE_MANAGER_STARTING = "imStarting";
    String INSTANCE_MANAGER_SHUTDOWN = "imShutdown";
    String INSTANCE_MANAGER_FAIL = "imFail";

    String INSTANCE_MANAGER_STARTING_ACK = "imStartingAck";

    String ORCHESTRATOR_STARTING = "orchStarting";
    String ORCHESTRATOR_SHUTDOWN = "orchShutdown";
    String ORCHESTRATOR_FAIL = "orchFail";

    String ORCHESTRATOR_STARTING_ACK = "orchStartingAck";

}
