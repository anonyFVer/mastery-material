package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Constants;
import java.util.HashMap;
import java.util.Map;
import static org.apache.dubbo.rpc.cluster.Constants.TAG_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.ALIVE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.CORE_THREADS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY_PREFIX;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METHODS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.QUEUES_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RELEASE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREADPOOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREADS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREAD_NAME_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.remoting.Constants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.INVOKER_LISTENER_KEY;
import static org.apache.dubbo.rpc.Constants.REFERENCE_FILTER_KEY;

public class ClusterUtils {

    private ClusterUtils() {
    }

    public static URL mergeUrl(URL remoteUrl, Map<String, String> localMap) {
        Map<String, String> map = new HashMap<String, String>();
        Map<String, String> remoteMap = remoteUrl.getParameters();
        if (remoteMap != null && remoteMap.size() > 0) {
            map.putAll(remoteMap);
            map.remove(THREAD_NAME_KEY);
            map.remove(DEFAULT_KEY_PREFIX + THREAD_NAME_KEY);
            map.remove(THREADPOOL_KEY);
            map.remove(DEFAULT_KEY_PREFIX + THREADPOOL_KEY);
            map.remove(CORE_THREADS_KEY);
            map.remove(DEFAULT_KEY_PREFIX + CORE_THREADS_KEY);
            map.remove(THREADS_KEY);
            map.remove(DEFAULT_KEY_PREFIX + THREADS_KEY);
            map.remove(QUEUES_KEY);
            map.remove(DEFAULT_KEY_PREFIX + QUEUES_KEY);
            map.remove(ALIVE_KEY);
            map.remove(DEFAULT_KEY_PREFIX + ALIVE_KEY);
            map.remove(Constants.TRANSPORTER_KEY);
            map.remove(DEFAULT_KEY_PREFIX + Constants.TRANSPORTER_KEY);
        }
        if (localMap != null && localMap.size() > 0) {
            String remoteGroup = map.get(GROUP_KEY);
            String remoteRelease = map.get(RELEASE_KEY);
            map.putAll(localMap);
            if (StringUtils.isNotEmpty(remoteGroup)) {
                map.put(GROUP_KEY, remoteGroup);
            }
            map.remove(RELEASE_KEY);
            if (StringUtils.isNotEmpty(remoteRelease)) {
                map.put(RELEASE_KEY, remoteRelease);
            }
        }
        if (remoteMap != null && remoteMap.size() > 0) {
            reserveRemoteValue(DUBBO_VERSION_KEY, map, remoteMap);
            reserveRemoteValue(VERSION_KEY, map, remoteMap);
            reserveRemoteValue(METHODS_KEY, map, remoteMap);
            reserveRemoteValue(TIMESTAMP_KEY, map, remoteMap);
            reserveRemoteValue(TAG_KEY, map, remoteMap);
            map.put(REMOTE_APPLICATION_KEY, remoteMap.get(APPLICATION_KEY));
            String remoteFilter = remoteMap.get(REFERENCE_FILTER_KEY);
            String localFilter = localMap.get(REFERENCE_FILTER_KEY);
            if (remoteFilter != null && remoteFilter.length() > 0 && localFilter != null && localFilter.length() > 0) {
                localMap.put(REFERENCE_FILTER_KEY, remoteFilter + "," + localFilter);
            }
            String remoteListener = remoteMap.get(INVOKER_LISTENER_KEY);
            String localListener = localMap.get(INVOKER_LISTENER_KEY);
            if (remoteListener != null && remoteListener.length() > 0 && localListener != null && localListener.length() > 0) {
                localMap.put(INVOKER_LISTENER_KEY, remoteListener + "," + localListener);
            }
        }
        return remoteUrl.clearParameters().addParameters(map);
    }

    private static void reserveRemoteValue(String key, Map<String, String> map, Map<String, String> remoteMap) {
        String remoteValue = remoteMap.get(key);
        if (StringUtils.isNotEmpty(remoteValue)) {
            map.put(key, remoteValue);
        }
    }
}