package com.bumptech.glide.integration.volley;

import com.android.volley.Request;
import com.android.volley.Request.Priority;
import com.bumptech.glide.load.data.DataFetcher.DataCallback;
import java.io.InputStream;
import java.util.Map;

public interface VolleyRequestFactory {

    Request<byte[]> create(String url, DataCallback<? super InputStream> callback, Priority priority, Map<String, String> headers);
}