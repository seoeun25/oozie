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
package org.apache.oozie.servlet;

import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.Callable;

public class TestV2AdminServlet extends DagServletTestCase {

    static {
        new V1AdminServlet();
        new V1JobServlet();
    }
    private static final boolean IS_SECURITY_ENABLED = false;

    @Override
    protected void setUp()throws Exception {
        super.setUp();
    }

    public void testV2QueueDump() throws Exception {
        runTest("/v2/admin/*", V2AdminServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                URL url = createURL(RestConstants.ADMIN_QUEUE_DUMP_RESOURCE, Collections.EMPTY_MAP);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONArray jsonArray = (JSONArray) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertTrue(jsonArray.size() > 0);

                for (Object obj: jsonArray) {
                    JSONObject queueInfo = (JSONObject) obj;
                    assertTrue(queueInfo.containsKey(JsonTags.OOZIE_HOST));
                    assertTrue(queueInfo.containsKey(JsonTags.QUEUE_DUMP));
                }
                return null;
            }
        });

    }

}
