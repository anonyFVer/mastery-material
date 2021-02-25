package com.bumptech.glide.load.model;

import android.content.Context;
import android.net.Uri;
import android.os.ParcelFileDescriptor;
import com.bumptech.glide.load.Options;
import java.io.File;
import java.io.InputStream;

public class StringLoader<Data> implements ModelLoader<String, Data> {

    private final ModelLoader<Uri, Data> uriLoader;

    public StringLoader(ModelLoader<Uri, Data> uriLoader) {
        this.uriLoader = uriLoader;
    }

    @Override
    public LoadData<Data> buildLoadData(String model, int width, int height, Options options) {
        Uri uri = parseUri(model);
        return uriLoader.buildLoadData(uri, width, height, options);
    }

    @Override
    public boolean handles(String model) {
        return true;
    }

    private static Uri parseUri(String model) {
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
        return uri;
    }

    private static Uri toFileUri(String path) {
        return Uri.fromFile(new File(path));
    }

    public static class StreamFactory implements ModelLoaderFactory<String, InputStream> {

        @Override
        public ModelLoader<String, InputStream> build(Context context, MultiModelLoaderFactory multiFactory) {
            return new StringLoader<>(multiFactory.build(Uri.class, InputStream.class));
        }

        @Override
        public void teardown() {
        }
    }

    public static class FileDescriptorFactory implements ModelLoaderFactory<String, ParcelFileDescriptor> {

        @Override
        public ModelLoader<String, ParcelFileDescriptor> build(Context context, MultiModelLoaderFactory multiFactory) {
            return new StringLoader<>(multiFactory.build(Uri.class, ParcelFileDescriptor.class));
        }

        @Override
        public void teardown() {
        }
    }
}