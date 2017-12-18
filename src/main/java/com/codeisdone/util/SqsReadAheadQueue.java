package com.codeisdone.util;

import java.lang.reflect.Method;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

/**
 * {@code SqsReadAheadQueue} implements a facade over an SQS queue that buffers in a local queue
 * messages that were sent to the AWS SQS queue specified in the builder that created an instance of
 * {@code SqsReadAheadQueue}.  {@code SqsReadAheadQueue} exists primarily to reduce the effective
 * latency for retrieving messages from an SQS queue.  It does this by continually fetching messages
 * asynchronously from the SQS queue and storing them into the local queue from which {@code SqsReadAheadQueue}
 * returns messages to callers for pop operations.  This ensures messages are available to your code as
 * quickly as possible.  Likewise, to improve responsiveness to callers, {@code SqsReadAheadQueue} uses an
 * asynchronous SQS client to send messages to SQS for push operations.
 * 
 * <p>While you could use the asynchronous SQS client and batch operations to effect some of the same
 * behavior as {@code SqsReadAheadQueue}, using those APIs would impose a specific usage pattern on your code, e.g.,
 * executing the interesting bits of your code in the thread SQS dispatched for a receive message callback or
 * belaying the execution to some other context.
 * 
 * <p>Via {@link Builder#withLocalQueueBlocking(boolean)}, you may indicate whether you want a {@code SqsReadAheadQueue}
 * instance to be blocking or non-blocking in regards to {@link #pop() and {@link #push(String)) operations.
 * Specifically, if configured as
 * 
 * <p><dl>
 * <dt>blocking
 * <dd>assumes the behavior of {@link java.util.concurrent.BlockingDeque#put(Object)} and {@link java.util.concurrent.BlockingDeque#take()}
 * 
 * <dt>non-blocking
 * <dd>assumes behavior of {@link java.util.Deque#offer(Object)} and {@link java.util.Deque#poll()}
 * </dl>
 * 
 * <p>The latter offers better concurrency though the former might better accommodate your code.
 * 
 * <p>Note that when {@code SqsReadAheadQueue} received a message from SQS and pushed it onto the local queue,
 * the clock would start running for the message visibility timeout.  Specifically, for {@code SqsReadAheadQueue},
 * any SQS message would be marked as in-flight for the duration between when {@code SqsReadAheadQueue} received
 * a message from SQS and the caller actually requested that message via a pop operation.  Therefore, be sure to consider
 * the value you specify in {@Link Builder#withSqsQueueMessageVisibilityTimeout(Integer)}.
 */
public class SqsReadAheadQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsReadAheadQueue.class);

    /** SQS queue reader thread name stem. */
    private static final String SQS_READER_THREAD_STEM = "fq-sqs-reader-";

    /** Maximum number of messages SQS allows you to receive in a single fetch. */
    private static final int SQS_MAX_RECEIVE_MESSAGE_COUNT = 10;

    /** Suffix for an SQS queue name that a FIFO queue. */
    private static final String SQS_QUEUE_NAME_SUFFIX_FIFO = ".fifo";
    
    /** Maximum number of in-flight messages SQS allow for a FIFO queue. */
    private static final int SQS_MAX_IN_FLIGHT_MESSAGES_FIFO = 20000;

    /** Maximum number of in-flight messages SQS allow for a stanard queue. */
    private static final int SQS_MAX_IN_FLIGHT_MESSAGES_STANDARD = 120000;

    /** Default for the maximum number of threads that will be dispatched to fill the local queue with messages in the SQS queue. */
    private static final int DEFAULT_LOCAL_QUEUE_FILE_THREADS = 20;

    /** AWS credentials to use. */
    private AWSCredentials awsCredentials;

    /** SQS queue name. */
    private String sqsQueueName;

    /** SQS queue URL. */
    private String sqsQueueUrl;

    /** SQS message visibility timeout. */
    private Integer sqsQueueMessageVisibilityTimeout;

    /** Maximum number of messages that will be buffered in the local queue. */
    private AtomicInteger localQueueMessageSlots;
    
    /** Maximum number of threads that will be dispatched to fill the local queue with messages in the SQS queue. */
    private final int localQueueFillThreads;

    /** Indicates whether local queue is blocking. */
    private boolean isLocalQueueBlocking;
    
    /** AWS client configuration to use with the SQS connection. */
    private ClientConfiguration awsClientConfiguration;
    
    /** AWS region to use with the SQS connection. */
    private String awsRegion;
    
    /** AWS endpoint configuration to use with the SQS connection. */
    private EndpointConfiguration awsEndpointConfiguration;
    
    /** SQS client. */
    private AmazonSQSAsync sqsClient;

    /** Indicates whether this instance has been terminated. */
    private volatile boolean isTerminated;
    
    /**
     * Local queue implementation that buffers SQS queue elements, where the objects include
     * 
     * <p><dl>
     * <dt>0
     * <dd>SQS message receipt handle
     * <dt>1
     * <dd>SQS message body
     * </dl>
     */
    private Deque<String[]> localQueue;

    /** Method to push elements onto local queue. */
    private Method push;
    
    /** Method to pop elements from local queue. */
    private Method pop;
    
    /**
     * Constructs instance using the specified builder.
     * 
     * @param builder builder with which to instantiate this
     */
    public SqsReadAheadQueue(Builder builder) {
        this.awsCredentials = builder.awsCredentials;
        this.sqsQueueName   = builder.sqsQueueName;
        this.sqsQueueMessageVisibilityTimeout = builder.sqsQueueMessageVisibilityTimeout;

        if (builder.localQueueMessageSlots == null) {
            if (sqsQueueName.endsWith(SQS_QUEUE_NAME_SUFFIX_FIFO)) {
                this.localQueueMessageSlots = new AtomicInteger(SQS_MAX_IN_FLIGHT_MESSAGES_FIFO);
            }
            else {
                this.localQueueMessageSlots = new AtomicInteger(SQS_MAX_IN_FLIGHT_MESSAGES_STANDARD);
            }
        }
        else {
            this.localQueueMessageSlots = new AtomicInteger(builder.localQueueMessageSlots);
        }
        
        if (builder.localQueueFillThreads == null) {
            this.localQueueFillThreads = DEFAULT_LOCAL_QUEUE_FILE_THREADS;
        }
        else {
            this.localQueueFillThreads = builder.localQueueFillThreads;
        }
        
        this.isLocalQueueBlocking     = builder.isLocalQueueBlocking;
        this.awsClientConfiguration   = builder.awsClientConfiguration;
        this.awsRegion                = builder.awsRegion;
        this.awsEndpointConfiguration = builder.awsEndpointConfiguration;
        
        initialize();
    }
    
    /**
     * Pushes the specified message onto the queue.
     * 
     * @param message message to push, perhaps an object serialized per JSON, for example
     */
    public void push(String message) {
        try {
            AsyncHandler<SendMessageRequest, SendMessageResult> resultHandler = new AsyncHandler<SendMessageRequest, SendMessageResult>() {
                @Override
                public void onError(Exception e) {
                    LOGGER.error("Exception encountered in sqs send message callback for queue {}", sqsQueueName, e);
                }
                
                @Override
                public void onSuccess(SendMessageRequest request, SendMessageResult result) {
                }
            };
            
            sqsClient.sendMessageAsync(sqsQueueUrl, message, resultHandler);
        }
        catch (Exception e) {
            LOGGER.error("Exception while sending message to sqs for queue {}", sqsQueueName, e);
        }
    }
    
    /**
     * Pops a message from the queue.
     * 
     * @returns message previously pushed onto queue or null, if no message were available when the call was made
     */
    public String pop() {
        String   message = null;
        String[] element = null;

        try {
            element = (String[]) pop.invoke(localQueue, (Object[]) null);
            
            if (element != null) {
                String messageReceipt = element[0];
                
                AsyncHandler<DeleteMessageRequest, DeleteMessageResult> resultHandler = new AsyncHandler<DeleteMessageRequest, DeleteMessageResult>() {
                    @Override
                    public void onError(Exception e) {
                        LOGGER.error("Exception encountered in sqs delete message callback for queue {}", sqsQueueName, e);
                    }
                    
                    @Override
                    public void onSuccess(DeleteMessageRequest request, DeleteMessageResult result) {
                    }
                };

                sqsClient.deleteMessageAsync(new DeleteMessageRequest(sqsQueueUrl, messageReceipt), resultHandler);

                message = element[1];
            }
        }
        catch (Exception e) {
            LOGGER.error("Exception while removing messages from sqs for queue {}", sqsQueueName, e);
        }
        finally {
            if (element != null) {
                localQueueMessageSlots.incrementAndGet();
            }
        }
        
        return message;
    }
    
    /**
     * Returns the URL of the SQS queue this instance references.
     * 
     * @return URL of SQS queue
     */
    public String getSQSQueueUrl() {
        return sqsQueueUrl;
    }

    /**
     * Performs per instance initialization, resolving the sqs connection and queue, and the local
     * queue and its methods.
     */
    private void initialize() {
        try { 
            sqsClient = AmazonSQSAsyncClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withClientConfiguration(awsClientConfiguration)
                .withRegion(awsRegion)
                .withEndpointConfiguration(awsEndpointConfiguration)
                .build();
        }
        catch (Exception e) {
            throw new IllegalStateException(String.format("SQS client connection was not resolved, sqsQueueName=%s", sqsQueueName), e);
        }

        try {
            GetQueueUrlResult getQueueResult = sqsClient.getQueueUrl(sqsQueueName);
            sqsQueueUrl = getQueueResult.getQueueUrl();
        }
        catch (QueueDoesNotExistException e) {
            throw new IllegalStateException(String.format("SQS queue was not resolved, sqsQueueName=%s", sqsQueueName), e);
        }

        if (isLocalQueueBlocking) {
            localQueue = new LinkedBlockingDeque<>();

            try {
                push = LinkedBlockingDeque.class.getMethod("put", Object.class);
            }
            catch (Exception e) {
                throw new IllegalStateException("Could not resolve blocking push method", e);
            }
            
            try {
                pop = LinkedBlockingDeque.class.getMethod("take", (Class<?>[]) null);
            }
            catch (Exception e) {
                throw new IllegalStateException("Could not resolve blocking pop method", e);
            }
        }
        else {
            localQueue = new ConcurrentLinkedDeque<>();

            try {
                push = ConcurrentLinkedDeque.class.getMethod("offer", Object.class);
            }
            catch (Exception e) {
                throw new IllegalStateException("Could not resolve non-blocking push method", e);
            }
            
            try {
                pop = ConcurrentLinkedDeque.class.getMethod("poll", (Class<?>[]) null);
            }
            catch (Exception e) {
                throw new IllegalStateException("Could not resolve non-blocking pop method", e);
            }
        }

        for (int i = 0; i < localQueueFillThreads; i++) {
            SqsQueueReader reader       = new SqsQueueReader();
            Thread         readerThread = new Thread(reader, SQS_READER_THREAD_STEM + i);
            
            readerThread.setDaemon(true);
            readerThread.start();
        }
    }
    
    /**
     * Terminates the queue, stopping all of its read-ahead threads and clearing its local queue.  It is
     * <em>not</em> necessary that you invoke {@code terminate()}.  It exists primarily to help ensure
     * orderly shutdown when the SQS queue an enclosing {@code SqsReadAheadQueue} references becomes
     * unavailable, for example, after the deletion of an SQS queue by a unit test.
     */
    public void terminate() {
        isTerminated = true;
        
        if (localQueue != null) {
            localQueue.clear();
        }
    }
    
    /**
     * {@code SqsQueueReader} implements a reader for elements in the SQS queue this instance references.
     * The reader receives messages from the SQS queue, placing them into the local queue.  However,
     * the reader does <em>not</em> delete the messages from the SQS queue, meaning the clock
     * begins running per the SQS visibility timeout.  Instead, when the reader pushes the
     * messages onto the local queue, it includes in each local queue element the SQS receipt handle and
     * the message body.  When {@code SqsReadAheadQueue} returns elements from the local queue to callers, it
     * uses the receipt handle to permanently delete the message from the SQS queue.
     */
    public class SqsQueueReader implements Runnable {
        @Override
        public void run() {
            while (true) {
                int numberOfMessages = 0;
                int numberOfSlots    = localQueueMessageSlots.get();
                
                if (numberOfSlots > 0) {
                    if (numberOfSlots > SQS_MAX_RECEIVE_MESSAGE_COUNT) {
                        numberOfMessages = SQS_MAX_RECEIVE_MESSAGE_COUNT;
                    }
                    else {
                        numberOfMessages = numberOfSlots;
                    }
                    
                    localQueueMessageSlots.set(numberOfSlots - numberOfMessages);

                    try {
                        ReceiveMessageRequest messageRequest = new ReceiveMessageRequest(sqsQueueUrl).
                            withMaxNumberOfMessages(numberOfMessages).
                            withWaitTimeSeconds(10).
                            withVisibilityTimeout(sqsQueueMessageVisibilityTimeout);
                    
                        if (!isTerminated) {
                            ReceiveMessageResult messageResult = sqsClient.receiveMessage(messageRequest);
                            List<Message>        messages      = messageResult.getMessages();
            
                            if (!messages.isEmpty()) {
                                for (Message message : messages) {
                                    push.invoke(localQueue, (Object) new String[]{message.getReceiptHandle(), message.getBody()});
                                }
                            }
                        }
                        else {
                            break;
                        }
                    }
                    catch (Exception e) {
                        LOGGER.error("Exception encountered in sqs receive message invocation for queue {}", sqsQueueName, e);
                    }
                }
            }
        }
    }
    
    /**
     * {@code Builder} implements a builder for {@code SqsReadAheadQueue}.
     */
    public static class Builder {
        /** AWS credentials to use. */
        private AWSCredentials awsCredentials;

        /** SQS queue name. */
        private String sqsQueueName;

        /** SQS message visibility timeout. */
        private Integer sqsQueueMessageVisibilityTimeout;

        /** Maximum number of messages that will be buffered in the local queue. */
        private Integer localQueueMessageSlots;

        /** Maximum number of threads that will be dispatched to fill the local queue with messages in the SQS queue. */
        private Integer localQueueFillThreads;
        
        /** Indicates whether local queue is blocking. */
        private boolean isLocalQueueBlocking;
        
        /** AWS client configuration to use with the SQS connection. */
        private ClientConfiguration awsClientConfiguration;
        
        /** AWS region to use with the SQS connection. */
        private String awsRegion;
        
        /** AWS endpoint configuration to use with the SQS connection. */
        private EndpointConfiguration awsEndpointConfiguration;
        
        /**
         * Constructs instance with mandatory properties.
         * 
         * @param awsCredentials AWS credentials to use&mdash;must not be null
         * @param sqsQueueName name of an <em>existing</em> SQS queue to use&mdash;must not be null or zero-length
         */
        public Builder(AWSCredentials awsCredentials, String sqsQueueName) {
            if (awsCredentials != null) {
                this.awsCredentials = awsCredentials;
            }
            else {
                throw new IllegalArgumentException("AWS credentials is null");
            }
            
            if (!StringUtils.isEmpty(sqsQueueName)) {
                this.sqsQueueName = sqsQueueName;
            }
            else {
                throw new IllegalArgumentException("SQS queue name is null or zero-length");
            }
        }
        
        /**
         * Sets the SQS queue message visibility timeout.  If not set, the value configured for the SQS queue will be used.
         * 
         * @param sqsQueueMessageVisibilityTimeout visibility timeout
         * 
         * @return this builder instance
         */
        public Builder withSqsQueueMessageVisibilityTimeout(Integer sqsQueueMessageVisibilityTimeout) {
            this.sqsQueueMessageVisibilityTimeout = sqsQueueMessageVisibilityTimeout;
            
            return this;
        }
        
        /**
         * Sets the number of slots, that is the message count limit, in the local queue that is used to buffer SQS
         * messages. If no value is specified for {@code localQueueMessageSlots}, the local queue will be limited to
         * the maximum number of in-flight messages that SQS imposes based on the type of queue:
         * 
         * <p><dl>
         * <dt>Standard
         * <dd>120,000
         * 
         * <dt>FIFO
         * <dd>20,000
         * </dl>
         * 
         * @param localQueueMessageSlots maximum number of messages that will be buffered in the local queue
         */
        public Builder withLocalQueueMessageSlots(Integer localQueueMessageSlots) {
            this.localQueueMessageSlots = localQueueMessageSlots;
            
            return this;
        }

        /**
         * Sets whether local queue is blocking.  If no value is specified for {@code isLocalQueueBlocking},
         * the queue will default to <em>non-blocking</em>.
         * 
         * @param isLocalQueueBlocking true to indicate blocking queue, false to create non-blocking queue
         */
        public Builder withLocalQueueBlocking(boolean isLocalQueueBlocking) {
            this.isLocalQueueBlocking = isLocalQueueBlocking;
            
            return this;
        }

        /**
         * Sets the the number of threads that will be dispatched to fill the local queue with messages
         * in the SQS queue.  If no value is specified for {@code reads}, it will default to <em>20</em>. 
         * 
         * @param localQueueFillThreads maximum number of threads that will be dispatched to fill the local queue with messages in the SQS queue
         */
        public Builder withLocalQueueFillThreads(Integer localQueueFillThreads) {
            this.localQueueFillThreads = localQueueFillThreads;
            
            return this;
        }

        /**
         * Sets the AWS client configuration to use with the SQS connection.
         * 
         * @param awsClientConfiguration AWS client configuration
         */
        public Builder withClientConfiguration(ClientConfiguration awsClientConfiguration) {
            this.awsClientConfiguration = awsClientConfiguration;
            
            return this;
        }
        
        /**
         * Sets the AWS region to use with the SQS connection.
         * 
         * <p><em>Note:</em> Per {@link com.amazonaws.client.builder.AwsClientBuilder#withRegion(Regions)},
         * since {@link com.amazonaws.regions.Regions}, at any point in time,
         * may not include an element for every region, {@code AwsClientBuilder} suggests using
         * {@link com.amazonaws.client.builder.AwsClientBuilder#withRegion(String)} overload to specify the
         * region.  Consequently, this method declares {@code awsRegion} as {@code String}. 
         * 
         * <p><em>Note:</em> Per AWS API, specify either region or endpoint configuration, not both.
         * 
         * @param awsRegion AWS region
         *
         * @see Builder#withEndpointConfiguration(EndpointConfiguration)
         * @see com.amazonaws.regions.Regions
         * @see com.amazonaws.client.builder.AwsClientBuilder#withRegion(String)}
         */
        public Builder withRegion(String awsRegion) {
            this.awsRegion = awsRegion;
            
            return this;
        }

        /** 
         * Sets the AWS endpoint configuration to use with the SQS connection.
         *
         * <p><em>Note:</em> Per AWS API, specify either region or endpoint configuration, not both.
          *
         * @param awsEndpointConfiguration AWS endpoint configuration
         * 
         * @see Builder#withRegion(String)
         * @see com.amazonaws.client.builder.AwsClientBuilder#withEndpointConfiguration(EndpointConfiguration)}
         */
        public Builder withEndpointConfiguration(EndpointConfiguration awsEndpointConfiguration) {
            this.awsEndpointConfiguration = awsEndpointConfiguration;
            
            return this;
        }

        /**
         * Returns the {@code SqsReadAheadQueue} constructed from this builder.
         * 
         * @return {@code SqsReadAheadQueue}
         */
        public SqsReadAheadQueue build() {
            return new SqsReadAheadQueue(this);
        }
    }
}
