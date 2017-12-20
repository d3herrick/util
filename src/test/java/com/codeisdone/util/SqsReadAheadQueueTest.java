/*
 * Copyright (c) 2017 Douglas Herrick
 * 
 * This file is subject to the license terms of https://github.com/d3herrick/util/blob/master/LICENSE
 */
package com.codeisdone.util;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.codeisdone.util.SqsReadAheadQueue;

/**
 * Unit test suite for {@link SqsReadAheadQueue}.
 * 
 * <p>To run this unit test suite, however you choose do it, you must define the following
 * as system properties:
 * 
 * <ul>
 * <li>aws.accesskey
 * <li>aws.secretkey
 * </ul>
 * 
 * <p>These properties define the AWS credentials that the unit test will use to create an SQS queue, send
 * and receive messages from that queue and, at the end of a unit test, delete that queue.  For example, to run
 * this unit test suite via maven, specify
 * 
 * <p>{@code mvn test -Dtest=SqsReadAheadQueueTest -Daws.accesskey=your-aws-accesskey-here -Daws.secretkey=your-aws-secretkey-here}
 */
public class SqsReadAheadQueueTest {
    private static Logger LOGGER = LoggerFactory.getLogger(SqsReadAheadQueueTest.class);
    
    private static AWSCredentials awsCredentials;
    private static AmazonSQS sqsClient; 

    @BeforeAll
    public static void oneTimeSetup() {
        awsCredentials = new BasicAWSCredentials(System.getProperty("aws.accesskey"), System.getProperty("aws.secretkey"));
        
        sqsClient = AmazonSQSClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
            .withRegion(Regions.US_EAST_1)
            .build();
    }
    
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class TestsWithSqsQueue {
        /**
         * Tests basic operations as blocking queue.
         */
        @Test
        public void testBlockingQueue(TestInfo testInfo) {
            String[]          sqsQueueRefs   = null;
            SqsReadAheadQueue readAheadQueue = null;

            try {
                sqsQueueRefs = createSqsQueue();
                
                // create queue specifying sqs region
                SqsReadAheadQueue.Builder builder = new SqsReadAheadQueue.Builder(awsCredentials, sqsQueueRefs[0])
                    .withSqsQueueMessageVisibilityTimeout(30)
                    .withRegion(Regions.US_EAST_1.getName())
                    .withLocalQueueFillThreads(1)
                    .withLocalQueueBlocking(true);
                
                readAheadQueue = builder.withLocalQueueMessageSlots(20).build();
                
                runQueueTest(testInfo.getDisplayName(), readAheadQueue, 1, 5);
            }
            catch (Exception e) {
                LOGGER.error("Spurious exception", e);
            }
            finally {
                deleteSqsQueue(sqsQueueRefs, readAheadQueue);
            }
        }
        
        /**
         * Tests basic operations as non-blocking queue.
         */
        @Test
        public void testNonBlockingQueue(TestInfo testInfo) {
            String[]          sqsQueueRefs   = null;
            SqsReadAheadQueue readAheadQueue = null;
            
            try {
                sqsQueueRefs = createSqsQueue();
                
                // create queue specifying sqs endpoint
                SqsReadAheadQueue.Builder builder = new SqsReadAheadQueue.Builder(awsCredentials, sqsQueueRefs[0])
                    .withSqsQueueMessageVisibilityTimeout(30)
                    .withEndpointConfiguration(new EndpointConfiguration("sqs.us-east-1.amazonaws.com", Regions.US_EAST_1.getName()))
                    .withLocalQueueFillThreads(2)
                    .withLocalQueueBlocking(false)
                    .withLocalQueueMessageSlots(100);
                
                readAheadQueue = builder.build();
                
                runQueueTest(testInfo.getDisplayName(), readAheadQueue, 1, 5);
            }
            catch (Exception e) {
                LOGGER.error("Spurious exception", e);
            }
            finally {
                deleteSqsQueue(sqsQueueRefs, readAheadQueue);
            }
        }

        /**
         * Creates an SQS queue.
         * 
         * @return array of String where first element is the SQS queue name, the second element the SQS queue URL
         */
        private String[] createSqsQueue() {
            String sqsQueueName = String.format("test_sraq_valid_%s", UUID.randomUUID().toString());
            String sqsQueueUrl  = null;
            
            try {
                sqsQueueUrl = (sqsClient.createQueue(new CreateQueueRequest(sqsQueueName))).getQueueUrl();
            }
            catch (Exception e) {
                throw new IllegalStateException(String.format("Could not create SQS queue %s", sqsQueueName), e);
            }
            
            return new String[] {sqsQueueName, sqsQueueUrl};
        }

        /**
         * Deletes the specified SQS queue and, if specified, terminates the read-ahead queue.
         * 
         * @param sqsQueueRefs SQS queue references
         * @param readAheadQueue read-ahead queue
         */
        private void deleteSqsQueue(String[] sqsQueueRefs, SqsReadAheadQueue readAheadQueue) {
            if (readAheadQueue != null) {
                readAheadQueue.terminate();
            }
            
            if (sqsQueueRefs != null) {
                try {
                    sqsClient.deleteQueue(new DeleteQueueRequest(sqsQueueRefs[1]));
                }
                catch (Exception e) {
                    throw new IllegalStateException(String.format("Could not delete SQS queue %s", sqsQueueRefs[1]), e);
                }
            }
        }
        
        /**
         * Populates specified queue with messages.
         * 
         * @param testName name of invoking test
         * @param queue queue to populate
         * @param workerThreadCount number of work threads to create
         * @param workerMessageCount number of messages to send per worker thread
         */
        private void runQueueTest(final String testName, final SqsReadAheadQueue queue, final int workerThreadCount, final int workerMessageCount) {
            final CountDownLatch queueLatch = new CountDownLatch(workerThreadCount << 1);
            
            for (int i = 0; i < workerThreadCount; i++) {
                Thread queueWriter = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (int i = 0; i < workerMessageCount; i++) {
                            queue.push("message-" + i);
                        }
                        
                        queueLatch.countDown();
                    }
                }, "queue-writer-" + i);
                
                queueWriter.setDaemon(true);
                queueWriter.start();
            }
    
            for (int i = 0; i < workerThreadCount; i++) {
                Thread queueReader = new Thread(new Runnable() {
                @Override
                    public void run() {
                        int popOpsAttempted = 0;
                        int popOpsSucceeded = 0;
                        int popOpsFailed    = 0;
                        
                        while (true) {
                            Object message = queue.pop();
                            
                            popOpsAttempted++;
                            
                            if (message == null) {
                                popOpsFailed++;
                            }
                            else {
                                popOpsSucceeded++;
                            }
                            
                            if (popOpsSucceeded == workerMessageCount) {
                                break;
                            }
                        }
    
                        LOGGER.info("Pop ops for {}: attempted={}, succeeded={}, failed={}", testName, popOpsAttempted, popOpsSucceeded, popOpsFailed);
                        
                        queueLatch.countDown();
                    }    
                }, "queue-reader-" + i);
                
                queueReader.setDaemon(true);
                queueReader.start();
            }
            
            try {
                queueLatch.await((workerThreadCount * workerMessageCount), TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                LOGGER.error("Queue operation interrupted", e);
            }
        }
    }
    
    @Nested
    class TestsWithoutSqsQueue {
        @Test
        public void testConstructionWithNullCredentials() {
            boolean isInvalid = false;
            
            try {
                new SqsReadAheadQueue.Builder(null, "some-queue-name");
            }
            catch (IllegalArgumentException e) {
                isInvalid = true;
            }
            
            Assertions.assertTrue(isInvalid, "construction should have failed for null aws credentials");
        }
        
        @Test
        public void testConstructionWithNullQueueName() {
            boolean isInvalid = false;
            
            try {
                new SqsReadAheadQueue.Builder(awsCredentials, null);
            }
            catch (IllegalArgumentException e) {
                isInvalid = true;
            }
            
            Assertions.assertTrue(isInvalid, "construction should have failed for empty queue name");
        }
        
        @Test
        public void testConstructionWithNonExistingQueueName() {
            boolean isInvalid = false;
            
            try {
                (new SqsReadAheadQueue.Builder(awsCredentials, String.format("test_sraq_invalid_%s", UUID.randomUUID().toString()))
                    .withRegion(Regions.US_EAST_1.getName()))
                    .build();
            }
            catch (IllegalStateException e) {
                isInvalid = true;
            }
            
            Assertions.assertTrue(isInvalid, "construction should have failed for non-existing queue name");
        }
    }
}
