/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.service;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.test.XTestCase;

import java.security.AccessControlException;
import java.util.Arrays;
import java.util.List;

public class TestProxyUserService extends XTestCase {

    public void testService() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",",
                            Arrays.asList(GroupsService.class.getName(), ProxyUserService.class.getName()))
            ));
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    public void testWrongConfigGroups() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName())),
                    "oozie.service.ProxyUserService.proxyuser.foo.hosts", "*"
            ));
            fail();
        }
        catch (ServiceException ex) {
        }
        catch (Exception ex) {
            fail();
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    public void testWrongHost() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName())),
                    "oozie.service.ProxyUserService.proxyuser.foo.hosts", "otherhost",
                    "oozie.service.ProxyUserService.proxyuser.foo.groups", "*"

            ));
            fail();
        }
        catch (ServiceException ex) {
        }
        catch (Exception ex) {
            fail();
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    public void testWrongConfigHosts() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName())),
                    "oozie.service.ProxyUserService.proxyuser.foo.groups", "*"

            ));
            fail();
        }
        catch (ServiceException ex) {
        }
        catch (Exception ex) {
            fail();
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    public void testValidateAnyHostAnyUser() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName())),
                    "oozie.service.ProxyUserService.proxyuser.foo.hosts", "*",
                    "oozie.service.ProxyUserService.proxyuser.foo.groups", "*"
            ));
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "localhost", "bar");
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    public void testInvalidProxyUser() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName())),
                    "oozie.service.ProxyUserService.proxyuser.foo.hosts", "*",
                    "oozie.service.ProxyUserService.proxyuser.foo.groups", "*"
            ));
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("bar", "localhost", "foo");
            fail();
        }
        catch (AccessControlException ex) {
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    public void testValidateHost() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName())),
                    "oozie.service.ProxyUserService.proxyuser.foo.hosts", "localhost",
                    "oozie.service.ProxyUserService.proxyuser.foo.groups", "*"
            ));
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "localhost", "bar");
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    private String getGroup() throws Exception {
        Services services = initNewServices(keyValueToProperties(
                Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName())),
                "server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName()))
        ));
        GroupsService groups = services.get(GroupsService.class);
        List<String> g = groups.getGroups(System.getProperty("user.name"));
        services.destroy();
        return g.get(0);
    }

    public void testValidateGroup() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName())),
                    "oozie.service.ProxyUserService.proxyuser.foo.hosts", "*",
                    "oozie.service.ProxyUserService.proxyuser.foo.groups", getGroup()
            ));
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "localhost", System.getProperty("user.name"));
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }


    public void testUnknownHost() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName())),
                    "oozie.service.ProxyUserService.proxyuser.foo.hosts", "localhost",
                    "oozie.service.ProxyUserService.proxyuser.foo.groups", "*"
            ));
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "unknownhost.bar.foo", "bar");
            fail();
        }
        catch (AccessControlException ex) {

        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    public void testInvalidHost() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName())),
                    "oozie.service.ProxyUserService.proxyuser.foo.hosts", "localhost",
                    "oozie.service.ProxyUserService.proxyuser.foo.groups", "*"
            ));
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "www.example.com", "bar");
            fail();
        }
        catch (AccessControlException ex) {

        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    public void testInvalidGroup() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName())),
                    "oozie.service.ProxyUserService.proxyuser.foo.hosts", "localhost",
                    "oozie.service.ProxyUserService.proxyuser.foo.groups", "nobody"
            ));
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "localhost", System.getProperty("user.name"));
            fail();
        }
        catch (AccessControlException ex) {

        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    public void testNullProxyUser() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                            ProxyUserService.class.getName()))
            ));
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate(null, "localhost", "bar");
            fail();
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("oozie.service.ProxyUserService.proxyuser.#USER#.hosts"));
            assertTrue(ex.getMessage().contains("oozie.service.ProxyUserService.proxyuser.#USER#.groups"));
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }

    public void testNullHost() throws Exception {
        Services services = null;
        try {
            services = initNewServices(keyValueToProperties(
                    Services.CONF_SERVICE_CLASSES, StringUtils.join(",",
                            Arrays.asList(GroupsService.class.getName(), ProxyUserService.class.getName()))
            ));
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", null, "bar");
            fail();
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("oozie.service.ProxyUserService.proxyuser.foo.hosts"));
            assertTrue(ex.getMessage().contains("oozie.service.ProxyUserService.proxyuser.foo.groups"));
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            if (services != null) {
                services.destroy();
            }
        }
    }
}

