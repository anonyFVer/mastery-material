package com.bumptech.glide.load;

import android.os.Build;

public enum DecodeFormat {

    ALWAYS_ARGB_8888, PREFER_RGB_565;

    public static final boolean REQUIRE_ARGB_8888 = Build.VERSION.SDK_INT >= Build.VERSION_CODES.KITKAT;

    public static final DecodeFormat DEFAULT = REQUIRE_ARGB_8888 ? ALWAYS_ARGB_8888 : PREFER_RGB_565;
}