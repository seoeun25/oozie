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

package org.apache.oozie.command.wf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.NotificationXCommand;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.*;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowInstance;
import org.junit.Assert;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.util.*;

public class TestWorkflowNotificationXCommand extends XDataTestCase {
    private XConfiguration jobConf = new XConfiguration();

    public static class CallbackServlet extends HttpServlet {
        public static volatile String JOB_ID = null;
        public static String NODE_NAME = null;
        public static String STATUS = null;
        public static String PARENT_ID = null;

        public static List<String> history = Collections.synchronizedList(new ArrayList<String>());

        public static void reset() {
            JOB_ID = null;
            NODE_NAME = null;
            STATUS = null;
            PARENT_ID = null;
            history.clear();
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            String jobId = req.getParameter("jobId");
            String nodeName = req.getParameter("nodeName");
            String status = req.getParameter("status");
            String parentId = req.getParameter("parentId");
            history.add(new NotificationEntity(jobId, nodeName, status).toString());
            JOB_ID = jobId;
            NODE_NAME = nodeName;
            STATUS = status;
            PARENT_ID = parentId;
            resp.setStatus(HttpServletResponse.SC_OK);
        }

    }

    private static class NotificationEntity {
        private String jobId;
        private String nodeName;
        private String status;
        public NotificationEntity(String jobId, String nodeName, String status ) {
            this.jobId = jobId;
            this.nodeName = nodeName;
            this.status = status;
        }
        public String toString() {
            return StringUtils.join(",", new String[]{jobId, nodeName, status});
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setSystemProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        XConfiguration oozieConf = new XConfiguration();
        oozieConf.set(NotificationXCommand.NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY, "10000");
        LocalOozie.start(oozieConf, "/hang/*", HangServlet.class, "/notification/*", CallbackServlet.class);
        CallbackServlet.reset();
        jobConf.clear();
    }

    @Override
    public void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public void testWFNotificationTimeout() throws Exception {
        LocalOozie.stop();
        XConfiguration oozieConf = new XConfiguration();
        oozieConf.set(NotificationXCommand.NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY, "50");
        LocalOozie.start(oozieConf, "/hang/*", HangServlet.class, "/notification/*", CallbackServlet.class);

        XConfiguration conf = new XConfiguration();
        conf.set(OozieClient.WORKFLOW_NOTIFICATION_URL, LocalOozie.getServletURL("/hang/*"));
        WorkflowInstance wfi = Mockito.mock(WorkflowInstance.class);
        Mockito.when(wfi.getConf()).thenReturn(conf);
        WorkflowJobBean workflow = Mockito.mock(WorkflowJobBean.class);
        Mockito.when(workflow.getId()).thenReturn("1");
        Mockito.when(workflow.getStatus()).thenReturn(WorkflowJob.Status.SUCCEEDED);
        Mockito.when(workflow.getWorkflowInstance()).thenReturn(wfi);
        WorkflowNotificationXCommand command = new WorkflowNotificationXCommand(workflow);
        command.setRetry(3);
        long start = System.currentTimeMillis();
        command.call();
        long end = System.currentTimeMillis();
        Assert.assertTrue(end - start >= 50);
        Assert.assertTrue(end - start < 10000);
    }

    public void testWFNotification() throws Exception {

        String notificationUrl = "/notification/wf?jobId=$jobId&parentId=$parentId";
        _testNotificationParentId(notificationUrl, "1", null, "");

        notificationUrl = "/notification/wf?jobId=$jobId";
        _testNotificationParentId(notificationUrl, "1", null, null);

        notificationUrl = "/notification/wf?jobId=$jobId&parentId=$parentId";
        _testNotificationParentId(notificationUrl, "1", "0000000-111111-oozie-XXX-C@1", "0000000-111111-oozie-XXX-C@1");

        notificationUrl = "/notification/wf?jobId=$jobId";
        _testNotificationParentId(notificationUrl, "1", "0000000-111111-oozie-XXX-C@1", null);

    }

    private void _testNotificationParentId(String notificationUrl, String jobId, String parentId, String expectedParentId)
            throws Exception{
        XConfiguration conf = new XConfiguration();
        conf.set(OozieClient.WORKFLOW_NOTIFICATION_URL, LocalOozie.getServletURL(notificationUrl));
        WorkflowInstance wfi = Mockito.mock(WorkflowInstance.class);
        Mockito.when(wfi.getConf()).thenReturn(conf);
        WorkflowJobBean workflow = Mockito.mock(WorkflowJobBean.class);
        Mockito.when(workflow.getId()).thenReturn(jobId);
        Mockito.when(workflow.getStatus()).thenReturn(WorkflowJob.Status.SUCCEEDED);
        Mockito.when(workflow.getParentId()).thenReturn(parentId);
        Mockito.when(workflow.getWorkflowInstance()).thenReturn(wfi);
        WorkflowNotificationXCommand command = new WorkflowNotificationXCommand(workflow);
        command.setRetry(3);
        command.call();

        Assert.assertEquals(jobId, CallbackServlet.JOB_ID);
        Assert.assertEquals(expectedParentId, CallbackServlet.PARENT_ID);
    }

    /**
     * Test : WORKFLOW_NOTIFICATION_URL and ACTION_NOTIFICATION_URL are configured.
     * @throws Exception
     */
    public void testQueueNotificationCommand1() throws Exception {
        jobConf.set(OozieClient.WORKFLOW_NOTIFICATION_URL,
                LocalOozie.getServletURL("/notification") + "/wf?jobId=$jobId&status=$status");
        jobConf.set(OozieClient.ACTION_NOTIFICATION_URL,
                LocalOozie.getServletURL("/notification") + "/wf?jobId=$jobId&nodeName=$nodeName&status=$status");

        // job.notification(RUNNING)
        // action.notification (start:T:end), action.start (end)
        // action.notification (end:T:null), job.notification(SUCCEED)
        String jobId = testActionSumbit(5 + 2); // 2 is for CompositeCallable

        for (String notification : CallbackServlet.history) {
            System.out.println("notification history = " + notification);
        }
        assertEquals(4, CallbackServlet.history.size());
        List<String> expectedNotification = Arrays.asList(jobId + ",null,RUNNING", jobId + ",:start:,T:end",
                jobId + ",end,T:null", jobId + ",null,SUCCEEDED");
        for (String notification: CallbackServlet.history) {
            assertTrue(expectedNotification.contains(notification));
        }
    }

    /**
     * Test : WORKFLOW_NOTIFICATION_URL is configured.
     * @throws Exception
     */
    public void testQueueNotificationCommand2() throws Exception {
        jobConf.set(OozieClient.WORKFLOW_NOTIFICATION_URL,
                LocalOozie.getServletURL("/notification") + "/wf?jobId=$jobId&status=$status");

        // job.notification(RUNNING)
        // action.start (end)
        // job.notification(SUCCEED)
        String jobId = testActionSumbit(3);
        for (String notification : CallbackServlet.history) {
            System.out.println("notification history = " + notification);
        }
        assertEquals(2, CallbackServlet.history.size());
        List<String> expectedNotification = Arrays.asList(jobId + ",null,RUNNING", jobId + ",null,SUCCEEDED");
        for (String notification: CallbackServlet.history) {
            assertTrue(expectedNotification.contains(notification));
        }
    }

    /**
     * Test : ACTION_NOTIFICATION_URL is configured.
     * @throws Exception
     */
    public void testQueueNotificationCommand3() throws Exception {
        jobConf.set(OozieClient.ACTION_NOTIFICATION_URL,
                LocalOozie.getServletURL("/notification") + "/wf?jobId=$jobId&nodeName=$nodeName&status=$status");

        // action.notification (start:T:end), action.start (end)
        // action.notification (end:T:null)
        String jobId = testActionSumbit(3 + 1);
        for (String notification : CallbackServlet.history) {
            System.out.println("notification history = " + notification);
        }
        assertEquals(2, CallbackServlet.history.size());
        List<String> expectedNotification = Arrays.asList(jobId + ",:start:,T:end", jobId + ",end,T:null");
        for (String notification: CallbackServlet.history) {
            assertTrue(expectedNotification.contains(notification));
        }
    }

    /**
     * Test : WORKFLOW_NOTIFICATION_URL and ACTION_NOTIFICATION_URL are not configured.
     * @throws Exception
     */
    public void testSkipNotificationCommand() throws Exception {
        testActionSumbit(1); // action.start
        assertEquals(0, CallbackServlet.history.size());
    }

    public String testActionSumbit(int expectedQueuingCommand) throws Exception {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='${appName}-foo'> " + "<start to='end' /> "
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, new File(URI.create(workflowUri)));
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("appName", "var-app-name");
        if (jobConf.size() > 0) {
            for (Map.Entry<String, String> entry: jobConf) {
                conf.set(entry.getKey(), entry.getValue());
            }
        }
        SubmitXCommand sc = new SubmitXCommand(conf);
        final String jobId = sc.call();
        new StartXCommand(jobId).call();

        waitFor(15 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQueryExecutor.WorkflowJobQuery.GET_WORKFLOW, jobId)
                        .getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        WorkflowJobBean wf = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQueryExecutor.WorkflowJobQuery.GET_WORKFLOW,
                jobId);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wf.getStatus());


        InstrumentationService instrumentationService = Services.get().get(InstrumentationService.class);
        Map<String, Map<String, Instrumentation.Element<Long>>> map = instrumentationService.get().getCounters();
        if (expectedQueuingCommand > 0) {
            assertTrue(map.containsKey("callablequeue"));
            Map<String, Instrumentation.Element<Long>> queueInstrumentation = map.get("callablequeue");
            assertTrue(queueInstrumentation.containsKey("queued"));
            long queued = queueInstrumentation.get("queued").getValue();
            assertEquals(expectedQueuingCommand, queued);
            CallableQueueService callableQueueService = Services.get().get(CallableQueueService.class);
            assertEquals(0, callableQueueService.getQueueDump().size());
        } else {
            assertFalse(map.containsKey("callablequeue"));
        }
        return jobId;
    }

    protected Configuration getWFConf(Path appPath) {
        Configuration conf = new Configuration();
        Path appUri = new Path(appPath, "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        if (jobConf.size() > 0) {
            for (Map.Entry<String,String> entry: jobConf) {
                conf.set(entry.getKey(), entry.getValue());
            }
        }
        return conf;
    }

    protected WorkflowActionBean createWorkflowAction(String wfId, String actionName, WorkflowAction.Status status,
                                                      boolean pending) throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        String actionname = "testAction";
        action.setName(actionname);
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfId, actionname));
        action.setJobId(wfId);
        action.setType("map-reduce");
        action.setTransition("transition");
        action.setStatus(status);
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setLastCheckTime(new Date());
        if (pending) {
            action.setPending();
        }
        else {
            action.resetPending();
        }

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        FileSystem fs = getFileSystem();
        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<configuration>"
                + "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName()
                + "</value></property>" + "<property><name>mapred.reducer.class</name><value>"
                + MapperReducerForTest.class.getName() + "</value></property>"
                + "<property><name>mapred.input.dir</name><value>" + inputDir.toString() + "</value></property>"
                + "<property><name>mapred.output.dir</name><value>" + outputDir.toString() + "</value></property>"
                + "</configuration>" + "</map-reduce>";
        action.setConf(actionXml);

        return action;
    }

}
