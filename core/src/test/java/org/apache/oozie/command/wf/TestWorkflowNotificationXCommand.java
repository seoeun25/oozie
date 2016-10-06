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
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.LauncherMapperHelper;
import org.apache.oozie.action.hadoop.MapReduceActionExecutor;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.NotificationXCommand;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PurgeService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.junit.Assert;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TestWorkflowNotificationXCommand extends XDataTestCase {
    private EmbeddedServletContainer container;
    private XConfiguration jobConfiguration = new XConfiguration();

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
        setSystemProperty(NotificationXCommand.NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY, "50");
        Services services = new Services();
        setClassesToBeExcluded(services.get(ConfigurationService.class).getConf(), new String[]{PurgeService.class.getName()});
        services.init();
        container = new EmbeddedServletContainer("blah");
        container.addServletEndpoint("/hang/*", HangServlet.class);
        //LocalOozie.start("/hang/*", HangServlet.class, "/notification/*", CallbackServlet.class);
        CallbackServlet.reset();
        jobConfiguration.clear();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            container.stop();
        }
        catch (Exception ex) {
        }
        try {
            Services.get().destroy();
        }
        catch (Exception ex) {
        }
        //LocalOozie.stop();
        super.tearDown();
    }

    public void testWFNotificationTimeout() throws Exception {
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

    public void testQueueNotificationCommand1() throws Exception {
        jobConfiguration.clear();
        jobConfiguration.set(OozieClient.WORKFLOW_NOTIFICATION_URL,
                "notification/wf?jobId=$jobId&status=$status");
        jobConfiguration.set(OozieClient.ACTION_NOTIFICATION_URL,
                "notification/wf?jobId=$jobId&nodeName=$nodeName&status=$status");

        // job.notification(RUNNING)
        // action.notification (start:T:end), action.start (end)
        // action.notification (end:T:null), job.notification(SUCCEED)
        testActionSumbit(5 + 2);
    }

    public void testQueueNotificationCommand2() throws Exception {
        jobConfiguration.clear();
        jobConfiguration.set(OozieClient.WORKFLOW_NOTIFICATION_URL,
                "notification/wf?jobId=$jobId&status=$status");

        // job.notification(RUNNING)
        // action.start (end)
        // job.notification(SUCCEED)
        testActionSumbit(3);
    }

    public void testQueueNotificationCommand3() throws Exception {
        jobConfiguration.clear();
        jobConfiguration.set(OozieClient.ACTION_NOTIFICATION_URL,
                "notification/wf?jobId=$jobId&nodeName=$nodeName&status=$status");

        // action.notification (start:T:end), action.start (end)
        // action.notification (end:T:null)
        testActionSumbit(3 + 1);
    }



    public void testSkipNotificationCommand() throws Exception {
        //testActionStart(0);
        testActionSumbit(1); // action.start
    }

    public void testActionStart(int expectedQueuingCommand) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP, true);
        WorkflowActionGetJPAExecutor wfActionGetCmd = new WorkflowActionGetJPAExecutor(action.getId());

        new ActionStartXCommand(action.getId(), "map-reduce").call();
        action = jpaService.execute(wfActionGetCmd);
        assertNotNull(action.getExternalId());

        ActionXCommand.ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(job, action, false, false);
        MapReduceActionExecutor actionExecutor = new MapReduceActionExecutor();
        JobConf conf = actionExecutor.createBaseHadoopConf(context, XmlUtils.parseXml(action.getConf()));
        String user = conf.get("user.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);

        String launcherId = action.getExternalId();
        final RunningJob launcherJob = jobClient.getJob(JobID.forName(launcherId));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());

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
    }

    public void testActionSumbit(int expectedQueuingCommand) throws Exception {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='${appName}-foo'> " + "<start to='end' /> "
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, new File(URI.create(workflowUri)));
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("appName", "var-app-name");
        if (jobConfiguration.size() > 0) {
            for (Map.Entry<String, String> entry: jobConfiguration) {
                conf.set(entry.getKey(), entry.getValue());
            }
        }
        SubmitXCommand sc = new SubmitXCommand(conf);
        final String jobId = sc.call();
        new StartXCommand(jobId).call();
        System.out.println("----- wf status 1 = " + WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQueryExecutor
                .WorkflowJobQuery.GET_WORKFLOW, jobId).getStatus());

        waitFor(15 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQueryExecutor.WorkflowJobQuery.GET_WORKFLOW, jobId)
                        .getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });
        System.out.println("----- wf status 2 = " + WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQueryExecutor
                .WorkflowJobQuery.GET_WORKFLOW, jobId).getStatus());
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
    }


    protected Configuration getWFConf(Path appPath) {
        Configuration conf = new Configuration();
        Path appUri = new Path(appPath, "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        if (jobConfiguration.size() > 0) {
            for (Map.Entry<String,String> entry: jobConfiguration) {
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

//    public void testQueueNotificationCommand() throws Exception {
//        _testQueueNotificationCommand(true, true, 6);
//        _testQueueNotificationCommand(true, false, 2);
//        _testQueueNotificationCommand(false, true, 4);
//        _testQueueNotificationCommand(false, false, 0);
//    }

    public void _testQueueNotificationCommand(boolean notifyWorkflowStatus, boolean notifyActionStatus, int expectedNotification)
            throws Exception {
        CallbackServlet.reset();
        final OozieClient wfClient = LocalOozie.getClient();
        OutputStream os = new FileOutputStream(getTestCaseDir() + "/config-default.xml");
        XConfiguration defaultConf = new XConfiguration();
        defaultConf.set("outputDir", "default-output-dir");
        defaultConf.writeXml(os);
        os.close();

        String workflowUri = getTestCaseFileUri("workflow.xml");
        String actionXml = "<map-reduce>"
                + "<job-tracker>${jobTracker}</job-tracker>"
                + "<name-node>${nameNode}</name-node>"
                + "        <prepare>"
                + "          <delete path=\"${nameNode}/user/${wf:user()}/mr/${outputDir}\"/>"
                + "        </prepare>"
                + "        <configuration>"
                + "          <property><name>bb</name><value>BB</value></property>"
                + "          <property><name>cc</name><value>from_action</value></property>"
                + "        </configuration>"
                + "      </map-reduce>";
        String wfXml = "<workflow-app xmlns=\"uri:oozie:workflow:0.5\" name=\"map-reduce-wf\">"
                + "    <start to=\"mr-node\"/>"
                + "    <action name=\"mr-node\">"
                + actionXml
                + "    <ok to=\"end\"/>"
                + "    <error to=\"fail\"/>"
                + "</action>"
                + "<kill name=\"fail\">"
                + "    <message>Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>"
                + "</kill>"
                + "<end name=\"end\"/>"
                + "</workflow-app>";

        writeToFile(wfXml, new File(URI.create(workflowUri)));
        Configuration conf = new XConfiguration();
        conf.set("nameNode", getNameNodeUri());
        conf.set("jobTracker", getJobTrackerUri());
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        if (notifyWorkflowStatus) {
            conf.set(OozieClient.WORKFLOW_NOTIFICATION_URL,
                    LocalOozie.getServletURL("/notification") + "/wf?jobId=$jobId&status=$status");
        }
        if (notifyActionStatus) {
            conf.set(OozieClient.ACTION_NOTIFICATION_URL,
                    LocalOozie.getServletURL("/notification") + "/wf?jobId=$jobId&nodeName=$nodeName&status=$status");
        }
        SubmitXCommand sc = new SubmitXCommand(conf);
        final String jobId = sc.call();
        new StartXCommand(jobId).call();
        waitFor(15 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return wfClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.KILLED;
            }
        });
        String actionId = jobId + "@mr-node";
        WorkflowActionBean action = WorkflowActionQueryExecutor.getInstance().get(WorkflowActionQueryExecutor.WorkflowActionQuery
                .GET_ACTION, actionId);
        Assert.assertEquals(WorkflowAction.Status.ERROR, action.getStatus());

        for (int i = 0; i < CallbackServlet.history.size(); i++) {
            System.out.println("history : " + CallbackServlet.history.get(i));
        }
        Assert.assertEquals(expectedNotification, CallbackServlet.history.size());
    }

}
