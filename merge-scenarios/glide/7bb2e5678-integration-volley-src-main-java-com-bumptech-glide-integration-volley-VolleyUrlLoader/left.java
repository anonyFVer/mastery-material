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

    private final RequestQueue requestQueue;

    public VolleyUrlLoader(RequestQueue requestQueue) {
        this.requestQueue = requestQueue;
    }

    @Override
    public DataFetcher<InputStream> getDataFetcher(GlideUrl url, int width, int height) {
        return new VolleyStreamFetcher(requestQueue, url, new VolleyRequestFuture<InputStream>());
    }

    @Override
    public boolean handles(GlideUrl url) {
        return true;
    }

    public static class Factory implements ModelLoaderFactory<GlideUrl, InputStream> {

        private static RequestQueue internalQueue;

        private RequestQueue requestQueue;

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

        public Factory(Context context) {
            this(getInternalQueue(context));
        }

        public Factory(RequestQueue requestQueue) {
            this.requestQueue = requestQueue;
        }

        @Override
        public ModelLoader<GlideUrl, InputStream> build(Context context, MultiModelLoaderFactory multiFactory) {
            return new VolleyUrlLoader(requestQueue);
        }

        @Override
        public void teardown() {
        }
    }
}