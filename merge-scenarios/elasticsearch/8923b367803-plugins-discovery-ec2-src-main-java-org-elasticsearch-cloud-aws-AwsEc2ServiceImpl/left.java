package org.elasticsearch.cloud.aws;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

public class AwsEc2ServiceImpl extends AbstractComponent implements AwsEc2Service, Closeable {

    public static final String EC2_METADATA_URL = "http://169.254.169.254/latest/meta-data/";

    private AmazonEC2Client client;

    public AwsEc2ServiceImpl(Settings settings) {
        super(settings);
    }

    @Override
    public synchronized AmazonEC2 client() {
        if (client != null) {
            return client;
        }
        this.client = new AmazonEC2Client(buildCredentials(logger, settings), buildConfiguration(logger, settings));
        String endpoint = findEndpoint(logger, settings);
        if (endpoint != null) {
            client.setEndpoint(endpoint);
        }
        return this.client;
    }

    protected static AWSCredentialsProvider buildCredentials(Logger logger, Settings settings) {
        AWSCredentialsProvider credentials;
        String key = CLOUD_EC2.KEY_SETTING.get(settings);
        String secret = CLOUD_EC2.SECRET_SETTING.get(settings);
        if (key.isEmpty() && secret.isEmpty()) {
            logger.debug("Using either environment variables, system properties or instance profile credentials");
            credentials = new DefaultAWSCredentialsProviderChain();
        } else {
            logger.debug("Using basic key/secret credentials");
            credentials = new StaticCredentialsProvider(new BasicAWSCredentials(key, secret));
        }
        return credentials;
    }

    protected static ClientConfiguration buildConfiguration(Logger logger, Settings settings) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(CLOUD_EC2.PROTOCOL_SETTING.get(settings));
        if (PROXY_HOST_SETTING.exists(settings) || CLOUD_EC2.PROXY_HOST_SETTING.exists(settings)) {
            String proxyHost = CLOUD_EC2.PROXY_HOST_SETTING.get(settings);
            Integer proxyPort = CLOUD_EC2.PROXY_PORT_SETTING.get(settings);
            String proxyUsername = CLOUD_EC2.PROXY_USERNAME_SETTING.get(settings);
            String proxyPassword = CLOUD_EC2.PROXY_PASSWORD_SETTING.get(settings);
            clientConfiguration.withProxyHost(proxyHost).withProxyPort(proxyPort).withProxyUsername(proxyUsername).withProxyPassword(proxyPassword);
        }
        String awsSigner = CLOUD_EC2.SIGNER_SETTING.get(settings);
        if (Strings.hasText(awsSigner)) {
            logger.debug("using AWS API signer [{}]", awsSigner);
            AwsSigner.configureSigner(awsSigner, clientConfiguration);
        }
        final Random rand = Randomness.get();
        RetryPolicy retryPolicy = new RetryPolicy(RetryPolicy.RetryCondition.NO_RETRY_CONDITION, new RetryPolicy.BackoffStrategy() {

            @Override
            public long delayBeforeNextRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception, int retriesAttempted) {
                logger.warn("EC2 API request failed, retry again. Reason was:", exception);
                return 1000L * (long) (10d * Math.pow(2, retriesAttempted / 2.0d) * (1.0d + rand.nextDouble()));
            }
        }, 10, false);
        clientConfiguration.setRetryPolicy(retryPolicy);
        return clientConfiguration;
    }

    protected static String findEndpoint(Logger logger, Settings settings) {
        String endpoint = null;
        if (CLOUD_EC2.ENDPOINT_SETTING.exists(settings)) {
            endpoint = CLOUD_EC2.ENDPOINT_SETTING.get(settings);
            logger.debug("using explicit ec2 endpoint [{}]", endpoint);
        } else if (REGION_SETTING.exists(settings) || CLOUD_EC2.REGION_SETTING.exists(settings)) {
            final String region = CLOUD_EC2.REGION_SETTING.get(settings);
            switch(region) {
                case "us-east-1":
                case "us-east":
                    endpoint = "ec2.us-east-1.amazonaws.com";
                    break;
                case "us-east-2":
                    endpoint = "ec2.us-east-2.amazonaws.com";
                    break;
                case "us-west":
                case "us-west-1":
                    endpoint = "ec2.us-west-1.amazonaws.com";
                    break;
                case "us-west-2":
                    endpoint = "ec2.us-west-2.amazonaws.com";
                    break;
                case "ap-southeast":
                case "ap-southeast-1":
                    endpoint = "ec2.ap-southeast-1.amazonaws.com";
                    break;
                case "ap-south":
                case "ap-south-1":
                    endpoint = "ec2.ap-south-1.amazonaws.com";
                    break;
                case "us-gov-west":
                case "us-gov-west-1":
                    endpoint = "ec2.us-gov-west-1.amazonaws.com";
                    break;
                case "ap-southeast-2":
                    endpoint = "ec2.ap-southeast-2.amazonaws.com";
                    break;
                case "ap-northeast":
                case "ap-northeast-1":
                    endpoint = "ec2.ap-northeast-1.amazonaws.com";
                    break;
                case "ap-northeast-2":
                    endpoint = "ec2.ap-northeast-2.amazonaws.com";
                    break;
                case "eu-west":
                case "eu-west-1":
                    endpoint = "ec2.eu-west-1.amazonaws.com";
                    break;
                case "eu-central":
                case "eu-central-1":
                    endpoint = "ec2.eu-central-1.amazonaws.com";
                    break;
                case "sa-east":
                case "sa-east-1":
                    endpoint = "ec2.sa-east-1.amazonaws.com";
                    break;
                case "cn-north":
                case "cn-north-1":
                    endpoint = "ec2.cn-north-1.amazonaws.com.cn";
                    break;
                default:
                    throw new IllegalArgumentException("No automatic endpoint could be derived from region [" + region + "]");
            }
            logger.debug("using ec2 region [{}], with endpoint [{}]", region, endpoint);
        }
        return endpoint;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.shutdown();
        }
        IdleConnectionReaper.shutdown();
    }
}