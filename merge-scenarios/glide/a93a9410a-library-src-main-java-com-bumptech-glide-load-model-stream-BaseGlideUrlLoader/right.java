package com.bumptech.glide.load.model.stream;

import android.text.TextUtils;
import com.bumptech.glide.load.Key;
import com.bumptech.glide.load.Options;
import com.bumptech.glide.load.model.GlideUrl;
import com.bumptech.glide.load.model.Headers;
import com.bumptech.glide.load.model.ModelCache;
import com.bumptech.glide.load.model.ModelLoader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class BaseGlideUrlLoader<Model> implements ModelLoader<Model, InputStream> {

    private final ModelLoader<GlideUrl, InputStream> concreteLoader;

    private final ModelCache<Model, GlideUrl> modelCache;

    protected BaseGlideUrlLoader(ModelLoader<GlideUrl, InputStream> concreteLoader) {
        this(concreteLoader, null);
    }

    protected BaseGlideUrlLoader(ModelLoader<GlideUrl, InputStream> concreteLoader, ModelCache<Model, GlideUrl> modelCache) {
        this.concreteLoader = concreteLoader;
        this.modelCache = modelCache;
    }

    @Override
    public LoadData<InputStream> buildLoadData(Model model, int width, int height, Options options) {
        GlideUrl result = null;
        if (modelCache != null) {
            result = modelCache.get(model, width, height);
        }
        if (result == null) {
            String stringURL = getUrl(model, width, height, options);
            if (TextUtils.isEmpty(stringURL)) {
                return null;
            }
            result = new GlideUrl(stringURL, getHeaders(model, width, height, options));
            if (modelCache != null) {
                modelCache.put(model, width, height, result);
            }
        }
        List<String> alternateUrls = getAlternateUrls(model, width, height, options);
        LoadData<InputStream> concreteLoaderData = concreteLoader.buildLoadData(result, width, height, options);
        if (alternateUrls.isEmpty()) {
            return concreteLoaderData;
        } else {
            return new LoadData<>(concreteLoaderData.sourceKey, getAlternateKeys(alternateUrls), concreteLoaderData.fetcher);
        }
    }

    private static List<Key> getAlternateKeys(List<String> alternateUrls) {
        List<Key> result = new ArrayList<>(alternateUrls.size());
        for (String alternate : alternateUrls) {
            result.add(new GlideUrl(alternate));
        }
        return result;
    }

    protected abstract String getUrl(Model model, int width, int height, Options options);

    protected List<String> getAlternateUrls(Model model, int width, int height, Options options) {
        return Collections.emptyList();
    }

    protected Headers getHeaders(Model model, int width, int height, Options options) {
        return Headers.NONE;
    }
}