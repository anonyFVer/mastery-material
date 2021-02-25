package com.bumptech.glide.integration.volley;

import com.android.volley.Request;
import com.android.volley.Request.Priority;
import java.io.InputStream;

public interface VolleyRequestFactory {

    Request<byte[]> create(String url, VolleyRequestFuture<InputStream> future, Priority priority);
}