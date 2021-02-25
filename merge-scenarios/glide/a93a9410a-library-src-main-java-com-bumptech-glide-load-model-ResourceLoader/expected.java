package com.bumptech.glide.load.model;

import android.content.ContentResolver;
import android.content.Context;
import android.content.res.Resources;
import android.net.Uri;
import android.os.ParcelFileDescriptor;
import android.util.Log;
import com.bumptech.glide.load.Options;
import java.io.InputStream;

public class ResourceLoader<Data> implements ModelLoader<Integer, Data> {

    private static final String TAG = "ResourceLoader";

    private final ModelLoader<Uri, Data> uriLoader;

    private final Resources resources;

    public ResourceLoader(Context context, ModelLoader<Uri, Data> uriLoader) {
        this(context.getResources(), uriLoader);
    }

    public ResourceLoader(Resources resources, ModelLoader<Uri, Data> uriLoader) {
        this.resources = resources;
        this.uriLoader = uriLoader;
    }

    @Override
    public LoadData<Data> buildLoadData(Integer model, int width, int height, Options options) {
        Uri uri = getResourceUri(model);
        return uri == null ? null : uriLoader.buildLoadData(uri, width, height, options);
    }

    private Uri getResourceUri(Integer model) {
        try {
            return Uri.parse(ContentResolver.SCHEME_ANDROID_RESOURCE + "://" + resources.getResourcePackageName(model) + '/' + resources.getResourceTypeName(model) + '/' + resources.getResourceEntryName(model));
        } catch (Resources.NotFoundException e) {
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Received invalid resource id: " + model, e);
            }
            return null;
        }
    }

    @Override
    public boolean handles(Integer model) {
        return true;
    }

    public static class StreamFactory implements ModelLoaderFactory<Integer, InputStream> {

        @Override
        public ModelLoader<Integer, InputStream> build(Context context, MultiModelLoaderFactory multiFactory) {
            return new ResourceLoader<>(context, multiFactory.build(Uri.class, InputStream.class));
        }

        @Override
        public void teardown() {
        }
    }

    public static class FileDescriptorFactory implements ModelLoaderFactory<Integer, ParcelFileDescriptor> {

        @Override
        public ModelLoader<Integer, ParcelFileDescriptor> build(Context context, MultiModelLoaderFactory multiFactory) {
            return new ResourceLoader<>(context, multiFactory.build(Uri.class, ParcelFileDescriptor.class));
        }

        @Override
        public void teardown() {
        }
    }
}