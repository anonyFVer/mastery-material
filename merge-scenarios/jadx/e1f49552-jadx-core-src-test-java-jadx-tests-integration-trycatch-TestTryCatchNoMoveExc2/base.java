package jadx.tests.integration.trycatch;

import org.junit.Test;
import jadx.core.dex.nodes.ClassNode;
import jadx.tests.api.SmaliTest;
import static jadx.tests.api.utils.JadxMatchers.containsLines;
import static jadx.tests.api.utils.JadxMatchers.containsOne;
import static org.junit.Assert.assertThat;

public class TestTryCatchNoMoveExc2 extends SmaliTest {

    @Test
    public void test() {
        ClassNode cls = getClassNodeFromSmaliWithPath("trycatch", "TestTryCatchNoMoveExc2");
        String code = cls.getCode().toString();
        assertThat(code, containsOne("try {"));
        assertThat(code, containsLines(2, "} catch (Exception unused) {", "}", "System.nanoTime();"));
    }
}