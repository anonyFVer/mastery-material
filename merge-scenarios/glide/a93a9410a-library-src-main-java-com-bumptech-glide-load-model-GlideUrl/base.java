package com.bumptech.glide.load.model;

import android.net.Uri;
import android.text.TextUtils;
import java.net.MalformedURLException;
import java.net.URL;

public class GlideUrl {

    private static final String ALLOWED_URI_CHARS = "@#&=*+-_.,:!?()/~'%";

    private final URL url;

    private String stringUrl;

    private URL safeUrl;

    public GlideUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("URL must not be null!");
        }
        this.url = url;
        stringUrl = null;
    }

    public GlideUrl(String url) {
        if (TextUtils.isEmpty(url)) {
            throw new IllegalArgumentException("String url must not be empty or null: " + url);
        }
        this.stringUrl = url;
        this.url = null;
    }

    public URL toURL() throws MalformedURLException {
        return getSafeUrl();
    }

    private URL getSafeUrl() throws MalformedURLException {
        if (safeUrl != null) {
            return safeUrl;
        }
        String unsafe = toString();
        String safe = Uri.encode(unsafe, ALLOWED_URI_CHARS);
        safeUrl = new URL(safe);
        return safeUrl;
    }

    @Override
    public String toString() {
        if (TextUtils.isEmpty(stringUrl)) {
            stringUrl = url.toString();
        }
        return stringUrl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}