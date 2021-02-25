package org.elasticsearch.search.aggregations.metrics.scripted;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalScriptedMetricTests extends InternalAggregationTestCase<InternalScriptedMetric> {

    private static final String REDUCE_SCRIPT_NAME = "reduceScript";

    private boolean hasReduceScript = randomBoolean();

    @Override
    protected InternalScriptedMetric createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        Map<String, Object> params = new HashMap<>();
        if (randomBoolean()) {
            params.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        Script reduceScript = null;
        if (hasReduceScript) {
            reduceScript = new Script(ScriptType.INLINE, MockScriptEngine.NAME, REDUCE_SCRIPT_NAME, params);
        }
        return new InternalScriptedMetric(name, randomAlphaOfLength(5), reduceScript, pipelineAggregators, metaData);
    }

    @Override
    protected ScriptService mockScriptService() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        @SuppressWarnings("unchecked")
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, Collections.singletonMap(REDUCE_SCRIPT_NAME, script -> {
            return ((List<Object>) script.get("_aggs")).size();
        }));
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singletonList(scriptEngine));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Collections.emptyList());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        try {
            return new ScriptService(settings, new Environment(settings), null, scriptEngineRegistry, scriptContextRegistry, scriptSettings);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    @Override
    protected void assertReduced(InternalScriptedMetric reduced, List<InternalScriptedMetric> inputs) {
        InternalScriptedMetric firstAgg = inputs.get(0);
        assertEquals(firstAgg.getName(), reduced.getName());
        assertEquals(firstAgg.pipelineAggregators(), reduced.pipelineAggregators());
        assertEquals(firstAgg.getMetaData(), reduced.getMetaData());
        if (hasReduceScript) {
            assertEquals(inputs.size(), reduced.aggregation());
        } else {
            assertEquals(inputs.size(), ((List<Object>) reduced.aggregation()).size());
        }
    }

    @Override
    protected Reader<InternalScriptedMetric> instanceReader() {
        return InternalScriptedMetric::new;
    }
}