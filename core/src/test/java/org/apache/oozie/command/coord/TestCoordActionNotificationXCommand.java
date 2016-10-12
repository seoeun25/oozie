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

package org.apache.oozie.command.coord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.NotificationXCommand;
import org.apache.oozie.command.wf.HangServlet;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PurgeService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XConfiguration;
import org.junit.Assert;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TestCoordActionNotificationXCommand extends XDataTestCase {
    private XConfiguration coordJobConf = new XConfiguration();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        coordJobConf.clear();
        setSystemProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        XConfiguration oozieConf = new XConfiguration();
        oozieConf.set(NotificationXCommand.NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY, "10000");
        oozieConf.set(NotificationXCommand.NOTIFICATION_MAX_RETRIES, "0");
        LocalOozie.start(oozieConf, new String[] {PurgeService.class.getCanonicalName()},
                "/hang/*", HangServlet.class, "/notification/*", NotificationServlet.class);
        NotificationServlet.reset();
    }

    @Override
    public void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public static class NotificationServlet extends HttpServlet {
        public static volatile String ACTION_ID = null;
        public static volatile String STATUS = null;

        public static List<String> history = Collections.synchronizedList(new ArrayList<String>());

        public static void reset() {
            ACTION_ID = null;
            STATUS = null;
            history.clear();
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            String actionId = req.getParameter("actionId");
            String status = req.getParameter("status");
            history.add(new NotificationEntity(actionId, status).toString());
            ACTION_ID = actionId;
            STATUS = status;
            resp.setStatus(HttpServletResponse.SC_OK);
        }
    }

    private static class NotificationEntity {
        private String actionId;
        private String status;
        public NotificationEntity(String actionId, String status) {
            this.actionId = actionId;
            this.status = status;
        }
        public String toString() {
            return StringUtils.join(",", new String[]{actionId, status});
        }
    }

    public void testCoordNotificationTimeout() throws Exception {
        LocalOozie.stop();
        XConfiguration oozieConf = new XConfiguration();
        oozieConf.set(NotificationXCommand.NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY, "50");
        LocalOozie.start(oozieConf, new String[]{PurgeService.class.getCanonicalName()},
                "/hang/*", HangServlet.class, "/notification/*", NotificationServlet.class);

        XConfiguration conf = new XConfiguration();
        conf.set(OozieClient.COORD_ACTION_NOTIFICATION_URL, LocalOozie.getServletURL("/hang/*"));
        String runConf = conf.toXmlString(false);
        CoordinatorActionBean coord = Mockito.mock(CoordinatorActionBean.class);
        Mockito.when(coord.getId()).thenReturn("1");
        Mockito.when(coord.getStatus()).thenReturn(CoordinatorAction.Status.SUCCEEDED);
        Mockito.when(coord.getRunConf()).thenReturn(runConf);
        CoordActionNotificationXCommand command = new CoordActionNotificationXCommand(coord);
        command.retries = 3;
        long start = System.currentTimeMillis();
        command.call();
        long end = System.currentTimeMillis();
        Assert.assertTrue(end - start >= 50);
        Assert.assertTrue(end - start <= 10000);
    }

    /**
     * Test the CoordActionNotification is queued when COORD_ACTION_NOTIFICATION_URL is configured.
     * @throws Exception
     */
    public void testQueueNotificationCommand() throws Exception {
        coordJobConf.set(OozieClient.COORD_ACTION_NOTIFICATION_URL,
                LocalOozie.getServletURL("/notification") + "/coord?actionId=$actionId&status=$status");

        // action.start
        // coord_action_notification
        // coord_action_ready
        String actionId = startCoordAction();
        long queued = instrumentaionOfQueued();
        assertEquals(3, queued);

        for (String notification : NotificationServlet.history) {
            System.out.println("notification history = " + notification);
        }
        assertEquals(1, NotificationServlet.history.size());
        assertEquals(actionId + ",RUNNING", NotificationServlet.history.get(0));
    }

    /**
     * Test the CoordActionNotification skip queueing when COORD_ACTION_NOTIFICATION_URL is not configured.
     * @throws Exception
     */
    public void testSkipNotificationCommand() throws Exception {
        // action.start
        // coord_action_ready
        startCoordAction();
        long queued = instrumentaionOfQueued();
        assertEquals(2, queued);
    }

    private String startCoordAction() throws Exception{
        Date start = DateUtils.parseDateOozieTZ("2009-12-15T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-12-16T01:00Z");
        CoordinatorJobBean coordJob = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, start, end, false, false, 1);

        CoordinatorActionBean action = addRecordToCoordActionTable(coordJob.getId(), 1,
                CoordinatorAction.Status.SUBMITTED, "coord-action-start-escape-strings.xml", 0);

        String actionId = action.getId();
        new CoordActionStartXCommand(actionId, getTestUser(), "myapp", "myjob").call();

        final JPAService jpaService = Services.get().get(JPAService.class);
        action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));

        if (action.getStatus() == CoordinatorAction.Status.SUBMITTED) {
            fail("CoordActionStartCommand didn't work because the status for action id" + actionId + " is :"
                    + action.getStatus() + " expected to be NOT SUBMITTED (i.e. RUNNING)");
        }

        final String wfId = action.getExternalId();

        waitFor(3 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                List<WorkflowActionBean> wfActions = jpaService.execute(new WorkflowActionsGetForJobJPAExecutor(wfId));
                return wfActions.size() > 0;
            }
        });
        List<WorkflowActionBean> wfActions = jpaService.execute(new WorkflowActionsGetForJobJPAExecutor(wfId));
        assertTrue(wfActions.size() > 0);

        // wait until all commands are executed.
        final CoordinatorActionBean actionBean = getCoordAction(actionId);
        waitFor(5 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return actionBean.getStatus() == CoordinatorAction.Status.SUCCEEDED;
            }
        });

        return actionId;
    }

    private long instrumentaionOfQueued() {
        long queued = 0;
        InstrumentationService instrumentationService = Services.get().get(InstrumentationService.class);
        Map<String, Map<String, Instrumentation.Element<Long>>> map = instrumentationService.get().getCounters();
        if (map.containsKey("callablequeue")) {
            Map<String, Instrumentation.Element<Long>> queueInstrumentation = map.get("callablequeue");
            if (queueInstrumentation.containsKey("queued")) {
                queued = queueInstrumentation.get("queued").getValue();
            }
        }
        return queued;
    }

    private CoordinatorActionBean getCoordAction(String actionId) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean actionBean;
        actionBean = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
        return actionBean;
    }

    protected Configuration getCoordConf(Path appPath) throws IOException {
        Configuration configuration = super.getCoordConf(appPath);
        if (coordJobConf.size() > 0) {
            for (Map.Entry<String, String> entry: coordJobConf) {
                configuration.set(entry.getKey(), entry.getValue());
            }
        }
        return configuration;
    }
}
