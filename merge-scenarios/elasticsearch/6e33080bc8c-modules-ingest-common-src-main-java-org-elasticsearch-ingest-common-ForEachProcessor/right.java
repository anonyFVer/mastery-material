package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.ingest.WrappingProcessor;
import org.elasticsearch.script.ScriptService;
import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readMap;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public final class ForEachProcessor extends AbstractProcessor implements WrappingProcessor {

    public static final String TYPE = "foreach";

    private final String field;

    private final Processor processor;

    private final boolean ignoreMissing;

    ForEachProcessor(String tag, String field, Processor processor, boolean ignoreMissing) {
        super(tag);
        this.field = field;
        this.processor = processor;
        this.ignoreMissing = ignoreMissing;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        List<?> values = ingestDocument.getFieldValue(field, List.class, ignoreMissing);
        if (values == null) {
            if (ignoreMissing) {
                return ingestDocument;
            }
            throw new IllegalArgumentException("field [" + field + "] is null, cannot loop over its elements.");
        }
        List<Object> newValues = new ArrayList<>(values.size());
        IngestDocument document = ingestDocument;
        for (Object value : values) {
            Object previousValue = ingestDocument.getIngestMetadata().put("_value", value);
            try {
                document = processor.execute(document);
                if (document == null) {
                    return null;
                }
            } finally {
                newValues.add(ingestDocument.getIngestMetadata().put("_value", previousValue));
            }
        }
        document.setFieldValue(field, newValues);
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    public Processor getInnerProcessor() {
        return processor;
    }

    public static final class Factory implements Processor.Factory {

        private final ScriptService scriptService;

        Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public ForEachProcessor create(Map<String, Processor.Factory> factories, String tag, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            boolean ignoreMissing = readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            Map<String, Map<String, Object>> processorConfig = readMap(TYPE, tag, config, "processor");
            Set<Map.Entry<String, Map<String, Object>>> entries = processorConfig.entrySet();
            if (entries.size() != 1) {
                throw newConfigurationException(TYPE, tag, "processor", "Must specify exactly one processor type");
            }
            Map.Entry<String, Map<String, Object>> entry = entries.iterator().next();
            Processor processor = ConfigurationUtils.readProcessor(factories, scriptService, entry.getKey(), entry.getValue());
            return new ForEachProcessor(tag, field, processor, ignoreMissing);
        }
    }
}