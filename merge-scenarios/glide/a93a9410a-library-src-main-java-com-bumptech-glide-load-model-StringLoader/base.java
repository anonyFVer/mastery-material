package com.bumptech.glide.load.model;

import android.net.Uri;
import com.bumptech.glide.load.data.DataFetcher;
import java.io.File;

public class StringLoader<T> implements ModelLoader<String, T> {

    private final ModelLoader<Uri, T> uriLoader;

    public StringLoader(ModelLoader<Uri, T> uriLoader) {
        this.uriLoader = uriLoader;
    }

    @Override
    public DataFetcher<T> getResourceFetcher(String model, int width, int height) {
        Uri uri;
        if (model.startsWith("/")) {
            uri = toFileUri(model);
        } else {
            uri = Uri.parse(model);
            final String scheme = uri.getScheme();
            if (scheme == null) {
                uri = toFileUri(model);
            }
        }
        return uriLoader.getResourceFetcher(uri, width, height);
    }

    private static Uri toFileUri(String path) {
        return Uri.fromFile(new File(path));
    }
}