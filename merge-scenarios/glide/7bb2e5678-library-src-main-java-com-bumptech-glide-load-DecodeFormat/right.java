package com.bumptech.glide.load;

public enum DecodeFormat {

    @Deprecated
    ALWAYS_ARGB_8888, PREFER_ARGB_8888, PREFER_RGB_565;

    public static final DecodeFormat DEFAULT = PREFER_RGB_565;
}