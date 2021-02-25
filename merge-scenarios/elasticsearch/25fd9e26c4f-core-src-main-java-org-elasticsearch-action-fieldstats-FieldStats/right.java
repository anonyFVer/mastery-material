package org.elasticsearch.action.fieldstats;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;

public abstract class FieldStats<T> implements Writeable, ToXContent {

    private final byte type;

    private long maxDoc;

    private long docCount;

    private long sumDocFreq;

    private long sumTotalTermFreq;

    private boolean isSearchable;

    private boolean isAggregatable;

    protected T minValue;

    protected T maxValue;

    FieldStats(byte type, long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq, boolean isSearchable, boolean isAggregatable, T minValue, T maxValue) {
        Objects.requireNonNull(minValue, "minValue must not be null");
        Objects.requireNonNull(maxValue, "maxValue must not be null");
        this.type = type;
        this.maxDoc = maxDoc;
        this.docCount = docCount;
        this.sumDocFreq = sumDocFreq;
        this.sumTotalTermFreq = sumTotalTermFreq;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    byte getType() {
        return this.type;
    }

    public String getDisplayType() {
        switch(type) {
            case 0:
                return "integer";
            case 1:
                return "float";
            case 2:
                return "date";
            case 3:
                return "string";
            case 4:
                return "ip";
            default:
                throw new IllegalArgumentException("Unknown type.");
        }
    }

    public long getMaxDoc() {
        return maxDoc;
    }

    public long getDocCount() {
        return docCount;
    }

    public int getDensity() {
        if (docCount < 0 || maxDoc <= 0) {
            return -1;
        }
        return (int) (docCount * 100 / maxDoc);
    }

    public long getSumDocFreq() {
        return sumDocFreq;
    }

    public long getSumTotalTermFreq() {
        return sumTotalTermFreq;
    }

    public boolean isSearchable() {
        return isSearchable;
    }

    public boolean isAggregatable() {
        return isAggregatable;
    }

    public T getMinValue() {
        return minValue;
    }

    public T getMaxValue() {
        return maxValue;
    }

    public abstract String getMinValueAsString();

    public abstract String getMaxValueAsString();

    protected abstract T valueOf(String value, String optionalFormat);

    public final void accumulate(FieldStats other) {
        this.maxDoc += other.maxDoc;
        if (other.docCount == -1) {
            this.docCount = -1;
        } else if (this.docCount != -1) {
            this.docCount += other.docCount;
        }
        if (other.sumDocFreq == -1) {
            this.sumDocFreq = -1;
        } else if (this.sumDocFreq != -1) {
            this.sumDocFreq += other.sumDocFreq;
        }
        if (other.sumTotalTermFreq == -1) {
            this.sumTotalTermFreq = -1;
        } else if (this.sumTotalTermFreq != -1) {
            this.sumTotalTermFreq += other.sumTotalTermFreq;
        }
        isSearchable |= other.isSearchable;
        isAggregatable |= other.isAggregatable;
        assert type == other.getType();
        updateMinMax((T) other.minValue, (T) other.maxValue);
    }

    private void updateMinMax(T min, T max) {
        if (compare(minValue, min) > 0) {
            minValue = min;
        }
        if (compare(maxValue, max) < 0) {
            maxValue = max;
        }
    }

    protected abstract int compare(T o1, T o2);

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD, getDisplayType());
        builder.field(MAX_DOC_FIELD, maxDoc);
        builder.field(DOC_COUNT_FIELD, docCount);
        builder.field(DENSITY_FIELD, getDensity());
        builder.field(SUM_DOC_FREQ_FIELD, sumDocFreq);
        builder.field(SUM_TOTAL_TERM_FREQ_FIELD, sumTotalTermFreq);
        builder.field(SEARCHABLE_FIELD, isSearchable);
        builder.field(AGGREGATABLE_FIELD, isAggregatable);
        toInnerXContent(builder);
        builder.endObject();
        return builder;
    }

    protected void toInnerXContent(XContentBuilder builder) throws IOException {
        builder.field(MIN_VALUE_FIELD, getMinValue());
        builder.field(MIN_VALUE_AS_STRING_FIELD, getMinValueAsString());
        builder.field(MAX_VALUE_FIELD, getMaxValue());
        builder.field(MAX_VALUE_AS_STRING_FIELD, getMaxValueAsString());
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type);
        out.writeLong(maxDoc);
        out.writeLong(docCount);
        out.writeLong(sumDocFreq);
        out.writeLong(sumTotalTermFreq);
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);
        writeMinMax(out);
    }

    protected abstract void writeMinMax(StreamOutput out) throws IOException;

    public boolean match(IndexConstraint constraint) {
        int cmp;
        T value = valueOf(constraint.getValue(), constraint.getOptionalFormat());
        if (constraint.getProperty() == IndexConstraint.Property.MIN) {
            cmp = compare(minValue, value);
        } else if (constraint.getProperty() == IndexConstraint.Property.MAX) {
            cmp = compare(maxValue, value);
        } else {
            throw new IllegalArgumentException("Unsupported property [" + constraint.getProperty() + "]");
        }
        switch(constraint.getComparison()) {
            case GT:
                return cmp > 0;
            case GTE:
                return cmp >= 0;
            case LT:
                return cmp < 0;
            case LTE:
                return cmp <= 0;
            default:
                throw new IllegalArgumentException("Unsupported comparison [" + constraint.getComparison() + "]");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        FieldStats<?> that = (FieldStats<?>) o;
        if (type != that.type)
            return false;
        if (maxDoc != that.maxDoc)
            return false;
        if (docCount != that.docCount)
            return false;
        if (sumDocFreq != that.sumDocFreq)
            return false;
        if (sumTotalTermFreq != that.sumTotalTermFreq)
            return false;
        if (isSearchable != that.isSearchable)
            return false;
        if (isAggregatable != that.isAggregatable)
            return false;
        if (!minValue.equals(that.minValue))
            return false;
        return maxValue.equals(that.maxValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, minValue, maxValue);
    }

    public static class Long extends FieldStats<java.lang.Long> {

        public Long(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq, boolean isSearchable, boolean isAggregatable, long minValue, long maxValue) {
            super((byte) 0, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, minValue, maxValue);
        }

        @Override
        public int compare(java.lang.Long o1, java.lang.Long o2) {
            return o1.compareTo(o2);
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            out.writeLong(minValue);
            out.writeLong(maxValue);
        }

        @Override
        public java.lang.Long valueOf(String value, String optionalFormat) {
            return java.lang.Long.parseLong(value);
        }

        @Override
        public String getMinValueAsString() {
            return java.lang.Long.toString(minValue);
        }

        @Override
        public String getMaxValueAsString() {
            return java.lang.Long.toString(maxValue);
        }
    }

    public static class Double extends FieldStats<java.lang.Double> {

        public Double(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq, boolean isSearchable, boolean isAggregatable, double minValue, double maxValue) {
            super((byte) 1, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, minValue, maxValue);
        }

        @Override
        public int compare(java.lang.Double o1, java.lang.Double o2) {
            return o1.compareTo(o2);
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            out.writeDouble(minValue);
            out.writeDouble(maxValue);
        }

        @Override
        public java.lang.Double valueOf(String value, String optionalFormat) {
            if (optionalFormat != null) {
                throw new UnsupportedOperationException("custom format isn't supported");
            }
            return java.lang.Double.parseDouble(value);
        }

        @Override
        public String getMinValueAsString() {
            return java.lang.Double.toString(minValue);
        }

        @Override
        public String getMaxValueAsString() {
            return java.lang.Double.toString(maxValue);
        }
    }

    public static class Date extends FieldStats<java.lang.Long> {

        private FormatDateTimeFormatter formatter;

        public Date(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq, boolean isSearchable, boolean isAggregatable, FormatDateTimeFormatter formatter, long minValue, long maxValue) {
            super((byte) 2, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, minValue, maxValue);
            this.formatter = formatter;
        }

        @Override
        public int compare(java.lang.Long o1, java.lang.Long o2) {
            return o1.compareTo(o2);
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            out.writeString(formatter.format());
            out.writeLong(minValue);
            out.writeLong(maxValue);
        }

        @Override
        public java.lang.Long valueOf(String value, String fmt) {
            FormatDateTimeFormatter f = formatter;
            if (fmt != null) {
                f = Joda.forPattern(fmt);
            }
            return f.parser().parseDateTime(value).getMillis();
        }

        @Override
        public String getMinValueAsString() {
            return formatter.printer().print(minValue);
        }

        @Override
        public String getMaxValueAsString() {
            return formatter.printer().print(maxValue);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            if (!super.equals(o))
                return false;
            Date that = (Date) o;
            return Objects.equals(formatter.format(), that.formatter.format());
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + formatter.format().hashCode();
            return result;
        }
    }

    public static class Text extends FieldStats<BytesRef> {

        public Text(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq, boolean isSearchable, boolean isAggregatable, BytesRef minValue, BytesRef maxValue) {
            super((byte) 3, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, minValue, maxValue);
        }

        @Override
        public int compare(BytesRef o1, BytesRef o2) {
            return o1.compareTo(o2);
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            out.writeBytesRef(minValue);
            out.writeBytesRef(maxValue);
        }

        @Override
        protected BytesRef valueOf(String value, String optionalFormat) {
            if (optionalFormat != null) {
                throw new UnsupportedOperationException("custom format isn't supported");
            }
            return new BytesRef(value);
        }

        @Override
        public String getMinValueAsString() {
            return minValue.utf8ToString();
        }

        @Override
        public String getMaxValueAsString() {
            return maxValue.utf8ToString();
        }

        @Override
        protected void toInnerXContent(XContentBuilder builder) throws IOException {
            builder.field(MIN_VALUE_FIELD, getMinValueAsString());
            builder.field(MAX_VALUE_FIELD, getMaxValueAsString());
        }
    }

    public static class Ip extends FieldStats<InetAddress> {

        public Ip(long maxDoc, long docCount, long sumDocFreq, long sumTotalTermFreq, boolean isSearchable, boolean isAggregatable, InetAddress minValue, InetAddress maxValue) {
            super((byte) 4, maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, minValue, maxValue);
        }

        @Override
        public int compare(InetAddress o1, InetAddress o2) {
            byte[] b1 = InetAddressPoint.encode(o1);
            byte[] b2 = InetAddressPoint.encode(o2);
            return StringHelper.compare(b1.length, b1, 0, b2, 0);
        }

        @Override
        public void writeMinMax(StreamOutput out) throws IOException {
            byte[] b1 = InetAddressPoint.encode(minValue);
            byte[] b2 = InetAddressPoint.encode(maxValue);
            out.writeByte((byte) b1.length);
            out.writeBytes(b1);
            out.writeByte((byte) b2.length);
            out.writeBytes(b2);
        }

        @Override
        public InetAddress valueOf(String value, String fmt) {
            return InetAddresses.forString(value);
        }

        @Override
        public String getMinValueAsString() {
            return NetworkAddress.format(minValue);
        }

        @Override
        public String getMaxValueAsString() {
            return NetworkAddress.format(maxValue);
        }
    }

    public static FieldStats readFrom(StreamInput in) throws IOException {
        byte type = in.readByte();
        long maxDoc = in.readLong();
        long docCount = in.readLong();
        long sumDocFreq = in.readLong();
        long sumTotalTermFreq = in.readLong();
        boolean isSearchable = in.readBoolean();
        boolean isAggregatable = in.readBoolean();
        switch(type) {
            case 0:
                return new Long(maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, in.readLong(), in.readLong());
            case 1:
                return new Double(maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, in.readDouble(), in.readDouble());
            case 2:
                FormatDateTimeFormatter formatter = Joda.forPattern(in.readString());
                return new Date(maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, formatter, in.readLong(), in.readLong());
            case 3:
                return new Text(maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, in.readBytesRef(), in.readBytesRef());
            case 4:
                int l1 = in.readByte();
                byte[] b1 = new byte[l1];
                in.readBytes(b1, 0, l1);
                int l2 = in.readByte();
                byte[] b2 = new byte[l2];
                in.readBytes(b2, 0, l2);
                InetAddress min = InetAddressPoint.decode(b1);
                InetAddress max = InetAddressPoint.decode(b2);
                return new Ip(maxDoc, docCount, sumDocFreq, sumTotalTermFreq, isSearchable, isAggregatable, min, max);
            default:
                throw new IllegalArgumentException("Unknown type.");
        }
    }

    static final String TYPE_FIELD = new String("type");

    static final String MAX_DOC_FIELD = new String("max_doc");

    static final String DOC_COUNT_FIELD = new String("doc_count");

    static final String DENSITY_FIELD = new String("density");

    static final String SUM_DOC_FREQ_FIELD = new String("sum_doc_freq");

    static final String SUM_TOTAL_TERM_FREQ_FIELD = new String("sum_total_term_freq");

    static final String SEARCHABLE_FIELD = new String("searchable");

    static final String AGGREGATABLE_FIELD = new String("aggregatable");

    static final String MIN_VALUE_FIELD = new String("min_value");

    static final String MIN_VALUE_AS_STRING_FIELD = new String("min_value_as_string");

    static final String MAX_VALUE_FIELD = new String("max_value");

    static final String MAX_VALUE_AS_STRING_FIELD = new String("max_value_as_string");
}