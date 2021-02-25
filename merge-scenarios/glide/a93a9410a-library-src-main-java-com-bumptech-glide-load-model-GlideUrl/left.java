package com.bumptech.glide.load.model;

import android.net.Uri;
import android.text.TextUtils;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

public class GlideUrl {

    private static final String ALLOWED_URI_CHARS = "@#&=*+-_.,:!?()/~'%";

    private final URL url;

    private final Headers headers;

    private final String stringUrl;

    private String safeStringUrl;

    private URL safeUrl;

    public GlideUrl(URL url) {
        this(url, Headers.NONE);
    }

    public GlideUrl(String url) {
        this(url, Headers.NONE);
    }

    public GlideUrl(URL url, Headers headers) {
        if (url == null) {
            throw new IllegalArgumentException("URL must not be null!");
        }
        if (headers == null) {
            throw new IllegalArgumentException("Headers must not be null");
        }
        this.url = url;
        stringUrl = null;
        this.headers = headers;
    }

    public GlideUrl(String url, Headers headers) {
        if (TextUtils.isEmpty(url)) {
            throw new IllegalArgumentException("String url must not be empty or null: " + url);
        }
        if (headers == null) {
            throw new IllegalArgumentException("Headers must not be null");
        }
        this.stringUrl = url;
        this.url = null;
        this.headers = headers;
    }

    public URL toURL() throws MalformedURLException {
        return getSafeUrl();
    }

    private URL getSafeUrl() throws MalformedURLException {
        if (safeUrl == null) {
            safeUrl = new URL(getSafeStringUrl());
        }
        return safeUrl;
    }

    public String toStringUrl() {
        return getSafeStringUrl();
    }

    private String getSafeStringUrl() {
        if (TextUtils.isEmpty(safeStringUrl)) {
            String unsafeStringUrl = stringUrl;
            if (TextUtils.isEmpty(unsafeStringUrl)) {
                unsafeStringUrl = url.toString();
            }
            safeStringUrl = Uri.encode(unsafeStringUrl, ALLOWED_URI_CHARS);
        }
        return safeStringUrl;
    }

    public Map<String, String> getHeaders() {
        return headers.getHeaders();
    }

    public String getCacheKey() {
        return stringUrl != null ? stringUrl : url.toString();
    }

    @Override
    public String toString() {
        return getCacheKey() + '\n' + headers.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof GlideUrl) {
            GlideUrl other = (GlideUrl) o;
            return getCacheKey().equals(other.getCacheKey()) && headers.equals(other.headers);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hashCode = getCacheKey().hashCode();
        hashCode = 31 * hashCode + headers.hashCode();
        return hashCode;
    }
}