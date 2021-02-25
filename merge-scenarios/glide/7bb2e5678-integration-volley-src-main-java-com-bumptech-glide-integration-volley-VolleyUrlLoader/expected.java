package com.bumptech.glide.integration.volley;

import android.content.Context;
import com.android.volley.RequestQueue;
import com.android.volley.toolbox.Volley;
import com.bumptech.glide.load.data.DataFetcher;
import com.bumptech.glide.load.model.GlideUrl;
import com.bumptech.glide.load.model.ModelLoader;
import com.bumptech.glide.load.model.ModelLoaderFactory;
import com.bumptech.glide.load.model.MultiModelLoaderFactory;
import java.io.InputStream;

public class VolleyUrlLoader implements ModelLoader<GlideUrl, InputStream> {

    public static class Factory implements ModelLoaderFactory<GlideUrl, InputStream> {

        private static volatile RequestQueue internalQueue;

        private final VolleyRequestFactory requestFactory;

        private final RequestQueue requestQueue;

        public Factory(Context context) {
            this(getInternalQueue(context));
        }

        public Factory(RequestQueue requestQueue) {
            this(requestQueue, VolleyStreamFetcher.DEFAULT_REQUEST_FACTORY);
        }

        public Factory(RequestQueue requestQueue, VolleyRequestFactory requestFactory) {
            this.requestFactory = requestFactory;
            this.requestQueue = requestQueue;
        }

        @Override
        public ModelLoader<GlideUrl, InputStream> build(Context context, MultiModelLoaderFactory factory) {
            return new VolleyUrlLoader(requestQueue, requestFactory);
        }

        @Override
        public void teardown() {
        }

        private static RequestQueue getInternalQueue(Context context) {
            if (internalQueue == null) {
                synchronized (Factory.class) {
                    if (internalQueue == null) {
                        internalQueue = Volley.newRequestQueue(context);
                    }
                }
            }
            return internalQueue;
        }
    }

    private final RequestQueue requestQueue;

    private final VolleyRequestFactory requestFactory;

    public VolleyUrlLoader(RequestQueue requestQueue) {
        this(requestQueue, VolleyStreamFetcher.DEFAULT_REQUEST_FACTORY);
    }

    public VolleyUrlLoader(RequestQueue requestQueue, VolleyRequestFactory requestFactory) {
        this.requestQueue = requestQueue;
        this.requestFactory = requestFactory;
    }

    @Override
    public boolean handles(GlideUrl url) {
        return true;
    }

    @Override
    public DataFetcher<InputStream> getDataFetcher(GlideUrl url, int width, int height) {
        return new VolleyStreamFetcher(requestQueue, url, new VolleyRequestFuture<InputStream>(), requestFactory);
    }
}