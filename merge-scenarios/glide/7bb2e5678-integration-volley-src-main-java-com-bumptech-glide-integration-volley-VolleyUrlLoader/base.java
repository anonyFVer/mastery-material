package com.bumptech.glide.integration.volley;

import android.content.Context;
import com.android.volley.RequestQueue;
import com.android.volley.toolbox.Volley;
import com.bumptech.glide.load.data.DataFetcher;
import com.bumptech.glide.load.model.GenericLoaderFactory;
import com.bumptech.glide.load.model.GlideUrl;
import com.bumptech.glide.load.model.ModelLoader;
import com.bumptech.glide.load.model.ModelLoaderFactory;
import java.io.InputStream;

public class VolleyUrlLoader implements ModelLoader<GlideUrl, InputStream> {

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
        public ModelLoader<GlideUrl, InputStream> build(Context context, GenericLoaderFactory factories) {
            return new VolleyUrlLoader(requestQueue);
        }

        @Override
        public void teardown() {
        }
    }

    private final RequestQueue requestQueue;

    public VolleyUrlLoader(RequestQueue requestQueue) {
        this.requestQueue = requestQueue;
    }

    @Override
    public DataFetcher<InputStream> getResourceFetcher(GlideUrl url, int width, int height) {
        return new VolleyStreamFetcher(requestQueue, url, new VolleyRequestFuture<InputStream>());
    }
}