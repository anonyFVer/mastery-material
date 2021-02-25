package org.apache.dubbo.metadata.store.redis;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.metadata.support.AbstractMetadataReport;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisMetadataReport extends AbstractMetadataReport {

    private final static Logger logger = LoggerFactory.getLogger(RedisMetadataReport.class);

    final JedisPool pool;

    public RedisMetadataReport(URL url) {
        super(url);
        pool = new JedisPool(new JedisPoolConfig(), url.getHost(), url.getPort());
    }

    @Override
    protected void doPut(URL url) {
        try (Jedis jedis = pool.getResource()) {
            jedis.set(getUrlKey(url), url.toParameterString());
        } catch (Throwable e) {
            logger.error("Failed to put " + url + " to redis " + url + ", cause: " + e.getMessage(), e);
            throw new RpcException("Failed to put " + url + " to redis " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    protected URL doPeek(URL url) {
        try (Jedis jedis = pool.getResource()) {
            String value = jedis.get(getUrlKey(url));
            if (value == null) {
                return null;
            }
            return url.addParameterString(value);
        } catch (Throwable e) {
            logger.error("Failed to peek " + url + " to redis " + url + ", cause: " + e.getMessage(), e);
            throw new RpcException("Failed to put " + url + " to redis " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }
}