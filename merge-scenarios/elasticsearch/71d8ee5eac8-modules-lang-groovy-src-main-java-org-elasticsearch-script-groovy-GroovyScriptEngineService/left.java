package org.elasticsearch.script.groovy;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyCodeSource;
import groovy.lang.Script;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.codehaus.groovy.ast.ClassCodeExpressionTransformer;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.expr.ConstantExpression;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.codehaus.groovy.control.messages.Message;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ClassPermission;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScoreAccessor;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.util.Collections.emptyList;

public class GroovyScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "groovy";

    public static final String GROOVY_INDY_SETTING_NAME = "indy";

    private final ClassLoader loader;

    public GroovyScriptEngineService(Settings settings) {
        super(settings);
        deprecationLogger.deprecated("[groovy] scripts are deprecated, use [painless] scripts instead");
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        this.loader = AccessController.doPrivileged((PrivilegedAction<ClassLoader>) () -> {
            AccessControlContext context = AccessController.getContext();
            return new ClassLoader(getClass().getClassLoader()) {

                @Override
                protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                    if (sm != null) {
                        try {
                            context.checkPermission(new ClassPermission(name));
                        } catch (SecurityException e) {
                            throw new ClassNotFoundException(name, e);
                        }
                    }
                    return super.loadClass(name, resolve);
                }
            };
        });
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public String getExtension() {
        return NAME;
    }

    @Override
    public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
        String className = MessageDigests.toHexString(MessageDigests.sha1().digest(scriptSource.getBytes(StandardCharsets.UTF_8)));
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                GroovyCodeSource codeSource = new GroovyCodeSource(scriptSource, className, BootstrapInfo.UNTRUSTED_CODEBASE);
                codeSource.setCachable(false);
                CompilerConfiguration configuration = new CompilerConfiguration().addCompilationCustomizers(new ImportCustomizer().addStarImports("org.joda.time").addStaticStars("java.lang.Math")).addCompilationCustomizers(new GroovyBigDecimalTransformer(CompilePhase.CONVERSION));
                configuration.getOptimizationOptions().put(GROOVY_INDY_SETTING_NAME, true);
                GroovyClassLoader groovyClassLoader = new GroovyClassLoader(loader, configuration);
                return groovyClassLoader.parseClass(codeSource);
            } catch (Exception e) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Exception compiling Groovy script:", e);
                }
                throw convertToScriptException("Error compiling script " + className, scriptSource, e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private Script createScript(Object compiledScript, Map<String, Object> vars) throws ReflectiveOperationException {
        Class<?> scriptClass = (Class<?>) compiledScript;
        Script scriptObject = (Script) scriptClass.getConstructor().newInstance();
        Binding binding = new Binding();
        binding.getVariables().putAll(vars);
        scriptObject.setBinding(binding);
        return scriptObject;
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
        deprecationLogger.deprecated("[groovy] scripts are deprecated, use [painless] scripts instead");
        try {
            Map<String, Object> allVars = new HashMap<>();
            if (vars != null) {
                allVars.putAll(vars);
            }
            return new GroovyScript(compiledScript, createScript(compiledScript.compiled(), allVars), this.logger);
        } catch (ReflectiveOperationException e) {
            throw convertToScriptException("Failed to build executable script", compiledScript.name(), e);
        }
    }

    @Override
    public SearchScript search(final CompiledScript compiledScript, final SearchLookup lookup, @Nullable final Map<String, Object> vars) {
        deprecationLogger.deprecated("[groovy] scripts are deprecated, use [painless] scripts instead");
        return new SearchScript() {

            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);
                Map<String, Object> allVars = new HashMap<>();
                allVars.putAll(leafLookup.asMap());
                if (vars != null) {
                    allVars.putAll(vars);
                }
                Script scriptObject;
                try {
                    scriptObject = createScript(compiledScript.compiled(), allVars);
                } catch (ReflectiveOperationException e) {
                    throw convertToScriptException("Failed to build search script", compiledScript.name(), e);
                }
                return new GroovyScript(compiledScript, scriptObject, leafLookup, logger);
            }

            @Override
            public boolean needsScores() {
                return true;
            }
        };
    }

    private ScriptException convertToScriptException(String message, String source, Throwable cause) {
        List<String> stack = new ArrayList<>();
        if (cause instanceof MultipleCompilationErrorsException) {
            @SuppressWarnings({ "unchecked" })
            List<Message> errors = (List<Message>) ((MultipleCompilationErrorsException) cause).getErrorCollector().getErrors();
            for (Message error : errors) {
                try (StringWriter writer = new StringWriter()) {
                    error.write(new PrintWriter(writer));
                    stack.add(writer.toString());
                } catch (IOException e1) {
                    logger.error("failed to write compilation error message to the stack", e1);
                }
            }
        } else if (cause instanceof CompilationFailedException) {
            CompilationFailedException error = (CompilationFailedException) cause;
            stack.add(error.getMessage());
        }
        throw new ScriptException(message, cause, stack, source, NAME);
    }

    public static final class GroovyScript implements ExecutableScript, LeafSearchScript {

        private final CompiledScript compiledScript;

        private final Script script;

        private final LeafSearchLookup lookup;

        private final Map<String, Object> variables;

        private final ESLogger logger;

        public GroovyScript(CompiledScript compiledScript, Script script, ESLogger logger) {
            this(compiledScript, script, null, logger);
        }

        @SuppressWarnings("unchecked")
        public GroovyScript(CompiledScript compiledScript, Script script, @Nullable LeafSearchLookup lookup, ESLogger logger) {
            this.compiledScript = compiledScript;
            this.script = script;
            this.lookup = lookup;
            this.logger = logger;
            this.variables = script.getBinding().getVariables();
        }

        @Override
        public void setScorer(Scorer scorer) {
            this.variables.put("_score", new ScoreAccessor(scorer));
        }

        @Override
        public void setDocument(int doc) {
            if (lookup != null) {
                lookup.setDocument(doc);
            }
        }

        @Override
        public void setNextVar(String name, Object value) {
            variables.put(name, value);
        }

        @Override
        public void setSource(Map<String, Object> source) {
            if (lookup != null) {
                lookup.source().setSource(source);
            }
        }

        @Override
        public Object run() {
            try {
                return AccessController.doPrivileged((PrivilegedAction<Object>) script::run);
            } catch (AssertionError ae) {
                final StackTraceElement[] elements = ae.getStackTrace();
                if (elements.length > 0 && "org.codehaus.groovy.runtime.InvokerHelper".equals(elements[0].getClassName())) {
                    logger.trace("failed to run {}", ae, compiledScript);
                    throw new ScriptException("Error evaluating " + compiledScript.name(), ae, emptyList(), "", compiledScript.lang());
                }
                throw ae;
            } catch (Exception | NoClassDefFoundError e) {
                logger.trace("failed to run {}", e, compiledScript);
                throw new ScriptException("Error evaluating " + compiledScript.name(), e, emptyList(), "", compiledScript.lang());
            }
        }

        @Override
        public long runAsLong() {
            return ((Number) run()).longValue();
        }

        @Override
        public double runAsDouble() {
            return ((Number) run()).doubleValue();
        }
    }

    private class GroovyBigDecimalTransformer extends CompilationCustomizer {

        private GroovyBigDecimalTransformer(CompilePhase phase) {
            super(phase);
        }

        @Override
        public void call(final SourceUnit source, final GeneratorContext context, final ClassNode classNode) throws CompilationFailedException {
            new BigDecimalExpressionTransformer(source).visitClass(classNode);
        }
    }

    private class BigDecimalExpressionTransformer extends ClassCodeExpressionTransformer {

        private final SourceUnit source;

        private BigDecimalExpressionTransformer(SourceUnit source) {
            this.source = source;
        }

        @Override
        protected SourceUnit getSourceUnit() {
            return this.source;
        }

        @Override
        public Expression transform(Expression expr) {
            Expression newExpr = expr;
            if (expr instanceof ConstantExpression) {
                ConstantExpression constExpr = (ConstantExpression) expr;
                Object val = constExpr.getValue();
                if (val != null && val instanceof BigDecimal) {
                    newExpr = new ConstantExpression(((BigDecimal) val).doubleValue());
                }
            }
            return super.transform(newExpr);
        }
    }
}