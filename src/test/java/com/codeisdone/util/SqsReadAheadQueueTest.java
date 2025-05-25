/*
MIT License

Copyright 2017-2020 Douglas Herrick

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
package com.codeisdone.util;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
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
         * Tests basic operations of queue instantiated with default properties.
         */
        @Test
        public void testDefaultQueue(TestInfo testInfo) {
            String[]          sqsQueueRefs   = null;
            SqsReadAheadQueue readAheadQueue = null;
            
            try {
                sqsQueueRefs = createSqsQueue();
                
                // create queue specifying only the region (specifying region eliminates dev environment setup dependencies)
                SqsReadAheadQueue.Builder builder = new SqsReadAheadQueue.Builder(awsCredentials, sqsQueueRefs[0])
                    .withAwsRegion(Regions.US_EAST_1.getName());
                
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
         * Tests basic operations as blocking queue.
         */
        @Test
        public void testBlockingQueue(TestInfo testInfo) {
            String[]          sqsQueueRefs   = null;
            SqsReadAheadQueue readAheadQueue = null;

            try {
                sqsQueueRefs = createSqsQueue();
                
                // create queue specifying sqs region with short polling (minimal wait time)
                SqsReadAheadQueue.Builder builder = new SqsReadAheadQueue.Builder(awsCredentials, sqsQueueRefs[0])
                    .withSqsQueueWaitTimeSeconds(1)
                    .withSqsQueueMessageVisibilityTimeout(30)
                    .withAwsRegion(Regions.US_EAST_1.getName())
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
                
                // create queue specifying sqs endpoint with long polling (maximum wait time)
                SqsReadAheadQueue.Builder builder = new SqsReadAheadQueue.Builder(awsCredentials, sqsQueueRefs[0])
                    .withSqsQueueMessageVisibilityTimeout(30)
                    .withAwsEndpointConfiguration(new EndpointConfiguration("sqs.us-east-1.amazonaws.com", Regions.US_EAST_1.getName()))
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
            
            BasicThreadFactory writerFactory = new BasicThreadFactory.Builder()
                .namingPattern("queue-writer-%d")
                .daemon(true)
                .build();
                
            for (int i = 0; i < workerThreadCount; i++) {
                writerFactory.newThread(new Runnable() {
                    @Override
                    public void run() {
                        for (int i = 0; i < workerMessageCount; i++) {
                            queue.push("message-" + i);
                        }
                        
                        queueLatch.countDown();
                    }
                }).start();
            }
    
            BasicThreadFactory readerFactory = new BasicThreadFactory.Builder()
                .namingPattern("queue-reader-%d")
                .daemon(true)
                .build();
 
            for (int i = 0; i < workerThreadCount; i++) {
                readerFactory.newThread(new Runnable() {
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
                }).start();
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
                    .withAwsRegion(Regions.US_EAST_1.getName()))
                    .build();
            }
            catch (IllegalStateException e) {
                isInvalid = true;
            }
            
            Assertions.assertTrue(isInvalid, "construction should have failed for non-existing queue name");
        }
    }
}
