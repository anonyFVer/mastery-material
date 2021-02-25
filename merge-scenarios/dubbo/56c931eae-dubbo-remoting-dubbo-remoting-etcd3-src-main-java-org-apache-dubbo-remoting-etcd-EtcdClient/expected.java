package org.apache.dubbo.remoting.etcd;

import org.apache.dubbo.common.URL;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface EtcdClient {

    void create(String path);

    long createEphemeral(String path);

    void delete(String path);

    List<String> getChildren(String path);

    List<String> addChildListener(String path, ChildListener listener);

    <T> T getChildListener(String path, ChildListener listener);

    void removeChildListener(String path, ChildListener listener);

    void addStateListener(StateListener listener);

    void removeStateListener(StateListener listener);

    boolean isConnected();

    void close();

    URL getUrl();

    long createLease(long second);

    long createLease(long ttl, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;

    void revokeLease(long lease);

    String getKVValue(String key);

    boolean put(String key, String value);

    boolean putEphemeral(String key, String value);
}