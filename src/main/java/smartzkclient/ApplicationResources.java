package smartzkclient;

/**
 * Created by Sophie on 17/10/2016.
 * This interface contains all agreements between Monitor, InstanceManager and Orchestrator.
 */
public interface ApplicationResources {
    String instanceManagerRootZnode = "instanceManagerRoot";
    String orchestratorRootZnode = "orchestratorRoot";
}
