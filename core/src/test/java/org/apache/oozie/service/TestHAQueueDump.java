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

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.servlet.HostnameFilter;
import org.apache.oozie.servlet.V2AdminServlet;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.ZKXTestCase;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;

public class TestHAQueueDump extends ZKXTestCase {

    EmbeddedServletContainer container;

    static {
        new V2AdminServlet();

    }

    protected void setUp() throws Exception {
        super.setUp();
        container = new EmbeddedServletContainer("oozie");
        container.addServletEndpoint("/v2/admin/*", V2AdminServlet.class);
        container.addServletEndpoint("/other-oozie-server/*", DummyV2AdminServlet.class);
        container.addFilter("*", HostnameFilter.class);
        container.start();
        Services.get().setService(ShareLibService.class);
        Services.get().getConf().setBoolean(AuthorizationService.CONF_SECURITY_ENABLED, false);

        Services.get().setService(ZKJobsConcurrencyService.class);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testQueueDumpWithHA() throws Exception {
        ZKJobsConcurrencyService zkjcs = new ZKJobsConcurrencyService();
        zkjcs.init(Services.get());
        DummyZKOozie dummyOozie_1 = null;
        DummyZKOozie dummyOozie_2 = null;
        try {
            dummyOozie_1 = new DummyZKOozie("9876", container.getServletURL("/other-oozie-server/*"));
            String url = container.getServletURL("/v2/admin/*") + "queue-dump?" + RestConstants.ALL_SERVER_REQUEST
                    + "=true";
            HttpClient client = new HttpClient();
            GetMethod method = new GetMethod(url);
            int statusCode = client.executeMethod(method);
            assertEquals(HttpURLConnection.HTTP_OK, statusCode);
            Reader reader = new InputStreamReader(method.getResponseBodyAsStream());
            JSONArray jsonArray = (JSONArray) JSONValue.parse(reader);
            assertEquals(2, jsonArray.size());
            // 1st server queue dump
            JSONObject obj = (JSONObject) jsonArray.get(0);

            JSONObject queueInfo = (JSONObject) obj;
            assertTrue(queueInfo.containsKey(JsonTags.OOZIE_HOST));
            assertTrue(queueInfo.containsKey(JsonTags.QUEUE_DUMP));
            assertTrue(queueInfo.containsKey(JsonTags.UNIQUE_MAP_DUMP));


            // 2nd server queue dump
            obj = (JSONObject) jsonArray.get(1);
            queueInfo = (JSONObject) obj;
            assertTrue(queueInfo.containsKey(JsonTags.OOZIE_HOST));
            assertTrue(queueInfo.containsKey(JsonTags.QUEUE_DUMP));
            assertTrue(queueInfo.containsKey(JsonTags.UNIQUE_MAP_DUMP));

            // 3rd server not defined. should throw exception.
            dummyOozie_2 = new DummyZKOozie("9873", container.getServletURL("/") + "not-defined/");

            statusCode = client.executeMethod(method);
            assertEquals(HttpURLConnection.HTTP_OK, statusCode);
            reader = new InputStreamReader(method.getResponseBodyAsStream());
            jsonArray = (JSONArray) JSONValue.parse(reader);
            assertEquals(3, jsonArray.size());

            // 3rd server queue dump
            obj = (JSONObject) jsonArray.get(2);
            queueInfo = (JSONObject) obj;
            assertTrue(queueInfo.containsKey(JsonTags.OOZIE_HOST));
            assertTrue(queueInfo.containsKey(JsonTags.QUEUE_DUMP));
            assertTrue(queueInfo.containsKey(JsonTags.UNIQUE_MAP_DUMP));

            for (Object jsonObj: jsonArray) {
                queueInfo = (JSONObject) jsonObj;
                assertTrue(queueInfo.containsKey(JsonTags.OOZIE_HOST));
                assertTrue(queueInfo.containsKey(JsonTags.QUEUE_DUMP));
                JSONArray queueDumpArray = (JSONArray) queueInfo.get(JsonTags.QUEUE_DUMP);

                if (queueInfo.get(JsonTags.OOZIE_HOST).toString().contains("not-defined")) {
                    for (Object o : queueDumpArray) {
                        JSONObject entry = (JSONObject) o;
                        if (entry.get(JsonTags.CALLABLE_DUMP) != null) {
                            String value = (String) entry.get(JsonTags.CALLABLE_DUMP);
                            assertTrue(value.contains("status: 404"));
                        }
                    }
                }
            }
        }
        finally {
            if (dummyOozie_1 != null) {
                dummyOozie_1.teardown();
            }

            if (dummyOozie_2 != null) {
                dummyOozie_2.teardown();
            }
            zkjcs.destroy();
            container.stop();
        }

    }

}
