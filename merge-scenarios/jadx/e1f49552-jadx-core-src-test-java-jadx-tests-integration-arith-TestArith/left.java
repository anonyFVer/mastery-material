package jadx.tests.integration.arith;

import org.junit.Test;
import jadx.core.dex.nodes.ClassNode;
import jadx.tests.api.IntegrationTest;

public class TestArith extends IntegrationTest {

    public static class TestCls {

        public int test(int a) {
            a += 2;
            use(a);
            return a;
        }

        public int test2(int a) {
            a++;
            use(a);
            return a;
        }

        private static void use(int i) {
        }
    }

    @Test
    public void test() {
        ClassNode cls = getClassNode(TestCls.class);
        String code = cls.getCode().toString();
    }

    @Test
    public void testNoDebug() {
        noDebugInfo();
        ClassNode cls = getClassNode(TestCls.class);
        String code = cls.getCode().toString();
    }
}