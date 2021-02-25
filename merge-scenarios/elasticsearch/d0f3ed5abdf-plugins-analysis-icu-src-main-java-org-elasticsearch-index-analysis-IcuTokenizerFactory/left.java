package org.elasticsearch.index.analysis;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UProperty;
import com.ibm.icu.lang.UScript;
import com.ibm.icu.text.RuleBasedBreakIterator;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.segmentation.DefaultICUTokenizerConfig;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizerConfig;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcuTokenizerFactory extends AbstractTokenizerFactory {

    private final ICUTokenizerConfig config;

    private static final String RULE_FILES = "rule_files";

    public IcuTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        config = getIcuConfig(environment, settings);
    }

    @Override
    public Tokenizer create() {
        if (config == null) {
            return new ICUTokenizer();
        } else {
            return new ICUTokenizer(config);
        }
    }

    private ICUTokenizerConfig getIcuConfig(Environment env, Settings settings) {
        Map<Integer, String> tailored = new HashMap<>();
        try {
            List<String> ruleFiles = settings.getAsList(RULE_FILES);
            for (String scriptAndResourcePath : ruleFiles) {
                int colonPos = scriptAndResourcePath.indexOf(":");
                if (colonPos == -1 || colonPos == scriptAndResourcePath.length() - 1) {
                    throw new IllegalArgumentException(RULE_FILES + " should contain comma-separated \"code:rulefile\" pairs");
                }
                String scriptCode = scriptAndResourcePath.substring(0, colonPos).trim();
                String resourcePath = scriptAndResourcePath.substring(colonPos + 1).trim();
                tailored.put(UCharacter.getPropertyValueEnum(UProperty.SCRIPT, scriptCode), resourcePath);
            }
            if (tailored.isEmpty()) {
                return null;
            } else {
                final RuleBasedBreakIterator[] breakers = new RuleBasedBreakIterator[UScript.CODE_LIMIT];
                for (Map.Entry<Integer, String> entry : tailored.entrySet()) {
                    int code = entry.getKey();
                    String resourcePath = entry.getValue();
                    breakers[code] = parseRules(resourcePath, env);
                }
                ICUTokenizerConfig config = new DefaultICUTokenizerConfig(true, true) {

                    @Override
                    public RuleBasedBreakIterator getBreakIterator(int script) {
                        if (breakers[script] != null) {
                            return (RuleBasedBreakIterator) breakers[script].clone();
                        } else {
                            return super.getBreakIterator(script);
                        }
                    }
                };
                return config;
            }
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load ICU rule files", e);
        }
    }

    private RuleBasedBreakIterator parseRules(String filename, Environment env) throws IOException {
        final Path path = env.configFile().resolve(filename);
        String rules = Files.readAllLines(path).stream().filter((v) -> v.startsWith("#") == false).collect(Collectors.joining("\n"));
        return new RuleBasedBreakIterator(rules.toString());
    }
}