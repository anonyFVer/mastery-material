package jadx.tests.external;

import java.io.File;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jadx.api.JadxArgs;
import jadx.api.JadxDecompiler;
import jadx.api.JadxInternalAccess;
import jadx.api.JavaClass;
import jadx.core.Jadx;
import jadx.core.ProcessClass;
import jadx.core.codegen.CodeWriter;
import jadx.core.dex.nodes.ClassNode;
import jadx.core.dex.nodes.MethodNode;
import jadx.core.dex.nodes.RootNode;
import jadx.core.dex.visitors.IDexTreeVisitor;
import jadx.core.utils.exceptions.JadxRuntimeException;
import jadx.tests.api.IntegrationTest;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public abstract class BaseExternalTest extends IntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(BaseExternalTest.class);

    protected abstract String getSamplesDir();

    protected JadxArgs prepare(String inputFile) {
        JadxArgs args = new JadxArgs();
        args.getInputFiles().add(new File(getSamplesDir(), inputFile));
        args.setOutDir(new File("../jadx-external-tests-tmp"));
        return args;
    }

    protected void decompile(JadxArgs jadxArgs) {
        decompile(jadxArgs, null, null);
    }

    protected void decompile(JadxArgs jadxArgs, String clsPatternStr) {
        decompile(jadxArgs, clsPatternStr, null);
    }

    protected void decompile(JadxArgs jadxArgs, @Nullable String clsPatternStr, @Nullable String mthPatternStr) {
        JadxDecompiler jadx = new JadxDecompiler(jadxArgs);
        jadx.load();
        if (clsPatternStr == null) {
            processAll(jadx);
        } else {
            Pattern clsPtrn = Pattern.compile(clsPatternStr);
            Pattern mthPtrn = mthPatternStr == null ? null : Pattern.compile(mthPatternStr);
            processByPatterns(jadx, clsPtrn, mthPtrn);
        }
        printErrorReport(jadx);
    }

    private void processAll(JadxDecompiler jadx) {
        for (JavaClass javaClass : jadx.getClasses()) {
            javaClass.decompile();
        }
    }

    private void processByPatterns(JadxDecompiler jadx, Pattern clsPattern, @Nullable Pattern mthPattern) {
        List<IDexTreeVisitor> passes = Jadx.getPassesList(jadx.getArgs());
        RootNode root = JadxInternalAccess.getRoot(jadx);
        int processed = 0;
        for (ClassNode classNode : root.getClasses(true)) {
            String clsFullName = classNode.getClassInfo().getFullName();
            if (clsPattern.matcher(clsFullName).matches()) {
                if (processCls(jadx, mthPattern, passes, classNode)) {
                    processed++;
                }
            }
        }
        assertThat("No classes processed", processed, greaterThan(0));
    }

    private boolean processCls(JadxDecompiler jadx, @Nullable Pattern mthPattern, List<IDexTreeVisitor> passes, ClassNode classNode) {
        classNode.load();
        boolean decompile = false;
        if (mthPattern == null) {
            decompile = true;
        } else {
            for (MethodNode mth : classNode.getMethods()) {
                if (mthPattern.matcher(mth.getName()).matches()) {
                    decompile = true;
                    break;
                }
            }
        }
        if (!decompile) {
            return false;
        }
        try {
            ProcessClass.process(classNode, passes, true);
        } catch (Exception e) {
            throw new JadxRuntimeException("Codegen failed", e);
        }
        LOG.warn("\n Print class: {}, {}", classNode.getFullName(), classNode.dex());
        if (mthPattern != null) {
            printMethods(classNode, mthPattern);
        } else {
            LOG.info("Code: \n{}", classNode.getCode());
        }
        checkCode(classNode);
        return true;
    }

    private void printMethods(ClassNode classNode, @NotNull Pattern mthPattern) {
        String code = classNode.getCode().getCodeStr();
        if (code == null) {
            return;
        }
        String[] lines = code.split(CodeWriter.NL);
        for (MethodNode mth : classNode.getMethods()) {
            if (mthPattern.matcher(mth.getName()).matches()) {
                int decompiledLine = mth.getDecompiledLine() - 1;
                StringBuilder mthCode = new StringBuilder();
                int startLine = getCommentLinesCount(lines, decompiledLine);
                int brackets = 0;
                for (int i = startLine; i > 0 && i < lines.length; i++) {
                    String line = lines[i];
                    mthCode.append(line).append(CodeWriter.NL);
                    if (i >= decompiledLine) {
                        brackets += StringUtils.countMatches(line, '{');
                        brackets -= StringUtils.countMatches(line, '}');
                        if (brackets <= 0) {
                            break;
                        }
                    }
                }
                LOG.info("Print method: {}\n{}", mth.getMethodInfo().getShortId(), mthCode);
            }
        }
    }

    protected int getCommentLinesCount(String[] lines, int line) {
        for (int i = line - 1; i > 0 && i < lines.length; i--) {
            String str = lines[i];
            if (str.isEmpty() || str.equals(CodeWriter.NL)) {
                return i + 1;
            }
        }
        return 0;
    }

    private void printErrorReport(JadxDecompiler jadx) {
        jadx.printErrorsReport();
        assertThat(jadx.getErrorsCount(), is(0));
    }
}