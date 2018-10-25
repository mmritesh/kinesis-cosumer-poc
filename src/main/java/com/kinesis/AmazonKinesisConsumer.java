package com.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Created by ritesh on 25/10/18.
 */

public class AmazonKinesisConsumer {



    private static final Log LOG = LogFactory.getLog(AmazonKinesisConsumer.class);

    protected AWSStaticCredentialsProvider awsStaticCredentialsProvider;

    private static final String STREAM_NAME = "testData";

    private static final String SAMPLE_APPLICATION_NAME = "services-user";

    private static ProfileCredentialsProvider credentialsProvider;

    private static void init() {
//        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }
    }

    public static void main(String[] args) throws Exception {
        init();
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
                        STREAM_NAME,
                        credentialsProvider,
                        workerId);

        kinesisClientLibConfiguration.withInitialPositionInStream(InitialPositionInStream.LATEST);
        IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();

        Worker worker = new Worker.Builder()
                            .recordProcessorFactory(recordProcessorFactory)
                            .config(kinesisClientLibConfiguration)
                            .build();
        LOG.info(String.format("Running %s to process stream %s as worker %s...", SAMPLE_APPLICATION_NAME, STREAM_NAME, workerId));
        int exitCode = 0;
        try {
            worker.run();
        } catch (Exception e) {
            LOG.info("Caught throwable while processing data.", e);
            exitCode = 1;
        }
        System.exit(exitCode);
    }
}
