package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import java.io.IOException;
import java.util.EnumSet;

public class CommonStatsFlags implements Streamable, Cloneable {

    public static final CommonStatsFlags ALL = new CommonStatsFlags().all();

    public static final CommonStatsFlags NONE = new CommonStatsFlags().clear();

    private EnumSet<Flag> flags = EnumSet.allOf(Flag.class);

    private String[] types = null;

    private String[] groups = null;

    private String[] fieldDataFields = null;

    private String[] completionDataFields = null;

    private boolean includeSegmentFileSizes = false;

    public CommonStatsFlags(Flag... flags) {
        if (flags.length > 0) {
            clear();
            for (Flag f : flags) {
                this.flags.add(f);
            }
        }
    }

    public CommonStatsFlags all() {
        flags = EnumSet.allOf(Flag.class);
        types = null;
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        return this;
    }

    public CommonStatsFlags clear() {
        flags = EnumSet.noneOf(Flag.class);
        types = null;
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        return this;
    }

    public boolean anySet() {
        return !flags.isEmpty();
    }

    public Flag[] getFlags() {
        return flags.toArray(new Flag[flags.size()]);
    }

    public CommonStatsFlags types(String... types) {
        this.types = types;
        return this;
    }

    public String[] types() {
        return this.types;
    }

    public CommonStatsFlags groups(String... groups) {
        this.groups = groups;
        return this;
    }

    public String[] groups() {
        return this.groups;
    }

    public CommonStatsFlags fieldDataFields(String... fieldDataFields) {
        this.fieldDataFields = fieldDataFields;
        return this;
    }

    public String[] fieldDataFields() {
        return this.fieldDataFields;
    }

    public CommonStatsFlags completionDataFields(String... completionDataFields) {
        this.completionDataFields = completionDataFields;
        return this;
    }

    public String[] completionDataFields() {
        return this.completionDataFields;
    }

    public CommonStatsFlags includeSegmentFileSizes(boolean includeSegmentFileSizes) {
        this.includeSegmentFileSizes = includeSegmentFileSizes;
        return this;
    }

    public boolean includeSegmentFileSizes() {
        return this.includeSegmentFileSizes;
    }

    public boolean isSet(Flag flag) {
        return flags.contains(flag);
    }

    boolean unSet(Flag flag) {
        return flags.remove(flag);
    }

    void set(Flag flag) {
        flags.add(flag);
    }

    public CommonStatsFlags set(Flag flag, boolean add) {
        if (add) {
            set(flag);
        } else {
            unSet(flag);
        }
        return this;
    }

    public static CommonStatsFlags readCommonStatsFlags(StreamInput in) throws IOException {
        CommonStatsFlags flags = new CommonStatsFlags();
        flags.readFrom(in);
        return flags;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        long longFlags = 0;
        for (Flag flag : flags) {
            longFlags |= (1 << flag.ordinal());
        }
        out.writeLong(longFlags);
        out.writeStringArrayNullable(types);
        out.writeStringArrayNullable(groups);
        out.writeStringArrayNullable(fieldDataFields);
        out.writeStringArrayNullable(completionDataFields);
        if (out.getVersion().onOrAfter(Version.V_5_0_0_alpha1)) {
            out.writeBoolean(includeSegmentFileSizes);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        final long longFlags = in.readLong();
        flags.clear();
        for (Flag flag : Flag.values()) {
            if ((longFlags & (1 << flag.ordinal())) != 0) {
                flags.add(flag);
            }
        }
        types = in.readStringArray();
        groups = in.readStringArray();
        fieldDataFields = in.readStringArray();
        completionDataFields = in.readStringArray();
        if (in.getVersion().onOrAfter(Version.V_5_0_0_alpha1)) {
            includeSegmentFileSizes = in.readBoolean();
        } else {
            includeSegmentFileSizes = false;
        }
    }

    @Override
    public CommonStatsFlags clone() {
        try {
            CommonStatsFlags cloned = (CommonStatsFlags) super.clone();
            cloned.flags = flags.clone();
            return cloned;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    public static enum Flag {

        Store("store"),
        Indexing("indexing"),
        Get("get"),
        Search("search"),
        Merge("merge"),
        Flush("flush"),
        Refresh("refresh"),
        QueryCache("query_cache"),
        FieldData("fielddata"),
        Docs("docs"),
        Warmer("warmer"),
        Completion("completion"),
        Segments("segments"),
        Translog("translog"),
        Suggest("suggest"),
        RequestCache("request_cache"),
        Recovery("recovery");

        private final String restName;

        Flag(String restName) {
            this.restName = restName;
        }

        public String getRestName() {
            return restName;
        }
    }
}