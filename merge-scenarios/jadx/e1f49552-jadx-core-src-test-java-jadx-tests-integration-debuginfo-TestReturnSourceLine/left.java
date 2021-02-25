package jadx.tests.integration.debuginfo;

import org.junit.Test;
import jadx.core.codegen.CodeWriter;
import jadx.core.dex.attributes.nodes.LineAttrNode;
import jadx.core.dex.nodes.ClassNode;
import jadx.core.dex.nodes.MethodNode;
import jadx.tests.api.IntegrationTest;
import static jadx.tests.api.utils.JadxMatchers.containsOne;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class TestReturnSourceLine extends IntegrationTest {

    public static class TestCls {

        public int test1(boolean v) {
            if (v) {
                f();
                return 1;
            }
            f();
            return 0;
        }

        private void f() {
        }
    }

    @Test
    public void test() {
        ClassNode cls = getClassNode(TestCls.class);
        CodeWriter codeWriter = cls.getCode();
        String code = codeWriter.toString();
        String[] lines = code.split(CodeWriter.NL);
        MethodNode test1 = cls.searchMethodByName("test1(Z)I");
        checkLine(lines, codeWriter, test1, 3, "return 1;");
    }

    private static void checkLine(String[] lines, CodeWriter cw, LineAttrNode node, int offset, String str) {
        int decompiledLine = node.getDecompiledLine() + offset;
        assertThat(lines[decompiledLine - 1], containsOne(str));
        Integer sourceLine = cw.getLineMapping().get(decompiledLine);
        assertNotNull(sourceLine);
        assertEquals(node.getSourceLine() + offset, (int) sourceLine);
    }
}