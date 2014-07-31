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
package org.apache.oozie.lock;

import org.apache.oozie.util.XLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * In memory resource locking that provides READ/WRITE lock capabilities.
 */
public class MemoryLocks {
    final private HashMap<String, ReentrantReadWriteLock> locks = new HashMap<String, ReentrantReadWriteLock>();

    private static enum Type {
        READ, WRITE
    }

    /**
     * Implementation of {@link LockToken} for in memory locks.
     */
    class MemoryLockToken implements LockToken {
        private final ReentrantReadWriteLock rwLock;
        private final java.util.concurrent.locks.Lock lock;
        private final String resource;

        private MemoryLockToken(ReentrantReadWriteLock rwLock, java.util.concurrent.locks.Lock lock, String resource) {
            this.rwLock = rwLock;
            this.lock = lock;
            this.resource = resource;
        }

        /**
         * Release the lock.
         */
        @Override
        public void release() {
            int val = rwLock.getQueueLength();
            if (val == 0) {
                synchronized (locks) {
                    locks.remove(resource);
                }
            }
            lock.unlock();
        }
    }

    /**
     * Return the number of active locks.
     *
     * @return the number of active locks.
     */
    public int size() {
        return locks.size();
    }

    /**
     * Obtain a READ lock for a source.
     *
     * @param resource resource name.
     * @param wait time out in milliseconds to wait for the lock, -1 means no timeout and 0 no wait.
     * @return the lock token for the resource, or <code>null</code> if the lock could not be obtained.
     * @throws InterruptedException thrown if the thread was interrupted while waiting.
     */
    public MemoryLockToken getReadLock(String resource, long wait) throws InterruptedException {
        return getLock(resource, Type.READ, wait);
    }

    /**
     * Obtain a WRITE lock for a source.
     *
     * @param resource resource name.
     * @param wait time out in milliseconds to wait for the lock, -1 means no timeout and 0 no wait.
     * @return the lock token for the resource, or <code>null</code> if the lock could not be obtained.
     * @throws InterruptedException thrown if the thread was interrupted while waiting.
     */
    public MemoryLockToken getWriteLock(String resource, long wait) throws InterruptedException {
        return getLock(resource, Type.WRITE, wait);
    }

    private MemoryLockToken getLock(String resource, Type type, long wait) throws InterruptedException {
        ReentrantReadWriteLock lockEntry;
        synchronized (locks) {
            if (locks.containsKey(resource)) {
                lockEntry = locks.get(resource);
            }
            else {
                lockEntry = new ReentrantReadWriteLock(true);
                locks.put(resource, lockEntry);
            }
        }

        Lock lock = (type.equals(Type.READ)) ? lockEntry.readLock() : lockEntry.writeLock();

        if (wait == -1) {
            lock.lock();
        }
        else {
            if (wait > 0) {
                if (!lock.tryLock(wait, TimeUnit.MILLISECONDS)) {
                    return null;
                }
            }
            else {
                if (!lock.tryLock()) {
                    return null;
                }
            }
        }
        synchronized (locks) {
            if (!locks.containsKey(resource)) {
                locks.put(resource, lockEntry);
            }
        }
        return new MemoryLockToken(lockEntry, lock, resource);
    }

    public void lockInfos(){
        XLog LOG = XLog.getLog(MemoryLocks.class);
        ReentrantReadWriteLock lockEntry;
        for (Map.Entry<String, ReentrantReadWriteLock> entry: locks.entrySet()) {
            LOG.info("  -- " + entry.getKey());
            System.out.println(" -------- " + entry.getKey());
            lockEntry = entry.getValue();
            Lock readLock = lockEntry.readLock();
            Lock writeLock = lockEntry.writeLock();
            LOG.info("---- readLock : " + readLock + ", writeLock : " + writeLock);
            System.out.println("---- lock : " + lockEntry.toString());
            System.out.println("---- readLock : "+ readLock.toString());
            System.out.println("---- writeLock : "+  writeLock.toString());
            int queueLength = lockEntry.getQueueLength();
            LOG.info("---- queueLength : " + queueLength);
            System.out.println("---- queueLength : " + queueLength);
        }
    }

    public Map<String,List<String>> getLockInfos(){
        Map<String,List<String>> lockInfos = new HashMap<String,List<String>>();
        ReentrantReadWriteLock lockEntry;
        for (Map.Entry<String, ReentrantReadWriteLock> entry: locks.entrySet()) {
            List<String> info = lockInfos.get(entry.getKey());
            if (info == null) {
                info = new ArrayList<String>();
                lockInfos.put(entry.getKey(), info);
            }
            lockEntry = entry.getValue();
            //info.add(extractInfo(lockEntry.toString()));
            info.add("Write lock = " + extractInfo(lockEntry.writeLock().toString()));
            info.add(extractInfo(lockEntry.readLock().toString()));
        }
        //test3();
        return lockInfos;
    }

    public String extractInfo(String lockInfo) {
        String pattern = "^.*?(\\[)(.*?)(\\])";
        StringBuilder sb = new StringBuilder();
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(lockInfo);
        if (lockInfo.matches(pattern)) {
            while (m.find()) {
                sb.append(m.group(2));
            }
        }
        else {
            sb.append(lockInfo);
        }
        return sb.toString();
    }

    public void test3() {
        String input = "User clientId=23421. Some more text clientId=33432. This clientNum=100";

        Pattern p = Pattern.compile("(clientId=)(\\d+)");
        Matcher m = p.matcher(input);

        StringBuffer result = new StringBuffer();
        while (m.find()) {
            System.out.println("Found a " + m.group() + ".");
            System.out.println("Masking: " + m.group(2));
            m.appendReplacement(result, m.group(1) + "***masked***");
        }
        m.appendTail(result);
        System.out.println(result);

        List<String> inputs = new ArrayList<String>();
        inputs.add("qq[abc]");
        inputs.add("sss[hello");
        inputs.add("[a=1,b=2]");
        inputs.add("[c=0]");
        inputs.add("[Locked by thread Thread-138]");


        //String pattern = "^\\[.*?\\]";
        String pattern = "^.*?(\\[)(.*?)(\\])";
        p = Pattern.compile(pattern);
        for (String ssn : inputs) {
            if (ssn.matches(pattern)) {
                System.out.println("Found good loc: " + ssn);
            }
            m = p.matcher(ssn);
            while (m.find()) {
                System.out.println("Found good lock : " + m.group(2));
            }
        }


    }

}
