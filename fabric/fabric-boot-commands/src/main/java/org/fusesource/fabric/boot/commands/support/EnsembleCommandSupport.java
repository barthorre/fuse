/**
 * Copyright (C) FuseSource, Inc.
 * http://fusesource.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.fabric.boot.commands.support;

import java.io.IOException;

import org.apache.felix.service.command.CommandSession;
import org.apache.karaf.shell.console.OsgiCommandSupport;
import org.fusesource.fabric.api.FabricService;
import org.fusesource.fabric.api.ZooKeeperClusterService;
import org.fusesource.fabric.utils.shell.ShellUtils;
import org.osgi.framework.ServiceReference;

/**
 */
public abstract class EnsembleCommandSupport extends OsgiCommandSupport {

    protected FabricService fabricService;
    protected ZooKeeperClusterService service;

    public ZooKeeperClusterService getService() {
        return service;
    }

    public void setService(ZooKeeperClusterService service) {
        this.service = service;
    }

    public FabricService getFabricService() {
        return fabricService;
    }

    public void setFabricService(FabricService fabricService) {
        this.fabricService = fabricService;
    }

    protected void checkFabricAvailable() {
    }

    protected boolean checkIfShouldModify(CommandSession session, boolean force) throws IOException {
        if (force) {
            return true;
        } else {
            String response = ShellUtils.readLine(session, "This will change of the zookeeper connection string.\nAre you sure want to proceed(yes/no):", false);
            if (response != null && (response.toLowerCase().equals("yes") || response.toLowerCase().equals("y"))) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Prompts the user for username and/or password.
     *
     * @param user     The default username.
     * @param password The default password.
     * @return An String array with username at index 0 and password at index 1.
     * @throws IOException
     */
    protected String[] promptForNewUser(String user, String password) throws IOException {
        String[] response = new String[2];
        // If the username was not configured via cli, then prompt the user for the values
        if (user == null || password == null) {
            System.out.println("No user found in etc/users.properties or specified as an option. Please specify one ...");
        }
        while (user == null || user.isEmpty()) {
            user = ShellUtils.readLine(session, "New user name: ", false);
            if (user == null) {
                break;
            }
        }

        if (user != null && password == null) {
            String password1 = null;
            String password2 = null;
            while (password1 == null || !password1.equals(password2)) {
                password1 = ShellUtils.readLine(session, "Password for " + user + ": ", true);
                password2 = ShellUtils.readLine(session, "Verify password for " + user + ":", true);
                
                if (password1 == null || password2 == null) {
                    break;
                }
                
                if (password1 != null && password1.equals(password2)) {
                    password = password1;
                } else {
                    System.out.println("Passwords did not match. Please try again!");
                }
            }
        }
        response[0] = user;
        response[1] = password;
        return response;
    }

}
