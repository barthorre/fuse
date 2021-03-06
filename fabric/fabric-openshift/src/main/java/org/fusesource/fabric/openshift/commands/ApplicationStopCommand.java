package org.fusesource.fabric.openshift.commands;

import com.openshift.client.IApplication;
import com.openshift.client.IDomain;
import com.openshift.client.IOpenShiftConnection;
import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Command;
import org.apache.felix.gogo.commands.Option;
import org.fusesource.fabric.openshift.commands.OpenshiftCommandSupport;

@Command(name = "application-stop", scope = "openshift", description = "Stops the target application")
public class ApplicationStopCommand extends OpenshiftCommandSupport {

    static final String FORMAT = "%-30s %s";

    @Option(name = "--domain", required = false, description = "Use only applications of that domain.")
    String domainId;

    @Argument(index = 0, name = "application", required = true, description = "The target application.")
    String applicationName;

    @Override
    protected Object doExecute() throws Exception {
        IOpenShiftConnection connection = getOrCreateConnection();
        for (IDomain domain : connection.getDomains()) {
            if (domainId == null || domainId.equals(domain.getId())) {
                IApplication application = domain.getApplicationByName(applicationName);
                application.stop();
            }
        }
        return null;
    }
}
