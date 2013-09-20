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

package org.fusesource.fabric.api;

public class CreateContainerChildOptions extends CreateContainerBasicOptions<CreateContainerChildOptions> {

    static final long serialVersionUID = 1148235037623812028L;

    private String jmxUser;
    private String jmxPassword;

    public CreateContainerChildOptions() {
        this.providerType = "child";
    }

    public CreateContainerChildOptions jmxUser(final String jmxUser) {
        this.setJmxUser(jmxUser);
        return this;
    }

    public CreateContainerChildOptions jmxPassword(final String jmxPassword) {
        this.setJmxPassword(jmxPassword);
        return this;
    }

    public String getJmxUser() {
        return jmxUser;
    }

    public void setJmxUser(String jmxUser) {
        this.jmxUser = jmxUser;
    }

    public String getJmxPassword() {
        return jmxPassword;
    }

    public void setJmxPassword(String jmxPassword) {
        this.jmxPassword = jmxPassword;
    }

    @Override
    public CreateContainerOptions updateCredentials(String user, String credential) {
        setJmxUser(user);
        setJmxPassword(credential);
        return this;
    }
}
