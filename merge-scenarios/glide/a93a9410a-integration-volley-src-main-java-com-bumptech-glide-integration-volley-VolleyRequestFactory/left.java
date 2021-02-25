package com.bumptech.glide.integration.volley;

import com.android.volley.Request;
import com.android.volley.Request.Priority;
import java.io.InputStream;
import java.util.Map;

public interface VolleyRequestFactory {

    Request<byte[]> create(String url, VolleyRequestFuture<InputStream> future, Priority priority, Map<String, String> headers);
}