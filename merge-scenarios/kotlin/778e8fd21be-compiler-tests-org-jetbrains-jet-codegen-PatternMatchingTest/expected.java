package org.jetbrains.jet.codegen;

import jet.NoPatternMatchedException;
import jet.Tuple2;

import java.lang.reflect.Method;

/**
 * @author yole
 */
public class PatternMatchingTest extends CodegenTestCase {
    @Override
    protected String getPrefix() {
        return "patternMatching";
    }

    public void testConstant() throws Exception {
        loadFile();
        Method foo = generateFunction();
        assertTrue((Boolean) foo.invoke(null, 0));
        assertFalse((Boolean) foo.invoke(null, 1));
    }

    public void testExceptionOnNoMatch() throws Exception {
        loadFile();
        Method foo = generateFunction();
        assertTrue((Boolean) foo.invoke(null, 0));
        assertThrows(foo, NoPatternMatchedException.class, null, 1);
    }

    public void testPattern() throws Exception {
        loadFile();
        Method foo = generateFunction();
        assertEquals("string", foo.invoke(null, ""));
        assertEquals("something", foo.invoke(null, new Object()));
    }

    public void testInrange() throws Exception {
        loadFile();
//        System.out.println(generateToText());
        Method foo = generateFunction();
        assertEquals("array list", foo.invoke(null, 239));
        assertEquals("digit", foo.invoke(null, 0));
        assertEquals("digit", foo.invoke(null, 9));
        assertEquals("digit", foo.invoke(null, 5));
        assertEquals("not small", foo.invoke(null, 190));
        assertEquals("something", foo.invoke(null, 19));
    }

    public void testIs() throws Exception {
        loadFile();
//        System.out.println(generateToText());
        blackBox();
    }

    public void testRange() throws Exception {
        loadFile();
//        System.out.println(generateToText());
        Method foo = generateFunction();
        assertEquals("array list", foo.invoke(null, 239));
        assertEquals("digit", foo.invoke(null, 0));
        assertEquals("digit", foo.invoke(null, 9));
        assertEquals("digit", foo.invoke(null, 5));
        assertEquals("something", foo.invoke(null, 19));
        assertEquals("not small", foo.invoke(null, 190));
    }

    public void testRangeChar() throws Exception {
        loadFile();
//        System.out.println(generateToText());
        Method foo = generateFunction();
        assertEquals("digit", foo.invoke(null, '0'));
        assertEquals("something", foo.invoke(null, 'A'));
    }

    public void testWildcardPattern() throws Exception {
        loadText("fun foo(x: String) = when(x) { is * => \"something\" }");
        Method foo = generateFunction();
        assertEquals("something", foo.invoke(null, ""));
    }

    public void testNoReturnType() throws Exception {
        loadText("fun foo(x: String) = when(x) { is * => \"x\" }");
        Method foo = generateFunction();
        assertEquals("x", foo.invoke(null, ""));
    }

    public void testTuplePattern() throws Exception {
        loadText("fun foo(x: (Any, Any)) = when(x) { is (1,2) => \"one,two\"; else => \"something\" }");
        Method foo = generateFunction();
        final Object result;
        try {
            result = foo.invoke(null, new Tuple2<Integer, Integer>(null, 1, 2));
        } catch (Exception e) {
            System.out.println(generateToText());
            throw e;
        }
        assertEquals("one,two", result);
        assertEquals("something", foo.invoke(null, new Tuple2<String, String>(null, "not", "tuple")));
    }

    // Commented for KT-621
//    public void testCall() throws Exception {
//        loadText("fun foo(s: String) = when(s) { .startsWith(\"J\") => \"JetBrains\"; else => \"something\" }");
//        Method foo = generateFunction();
//        try {
//            assertEquals("JetBrains", foo.invoke(null, "Java"));
//            assertEquals("something", foo.invoke(null, "C#"));
//        }
//        catch (Throwable t) {
//            System.out.println(generateToText());
//            t.printStackTrace();
//        }
//    }

    public void testCallProperty() throws Exception {
        blackBoxFile("patternMatching/callProperty.jet");
    }

    public void testNames() throws Exception {
        loadText("fun foo(x: (Any, Any)) = when(x) { is (val a is String, *) => a; else => \"something\" }");
        Method foo = generateFunction();
        assertEquals("JetBrains", foo.invoke(null, new Tuple2<String, String>(null, "JetBrains", "s.r.o.")));
        assertEquals("something", foo.invoke(null, new Tuple2<Integer, Integer>(null, 1, 2)));
    }

    public void testMultipleConditions() throws Exception {
        loadText("fun foo(x: Any) = when(x) { is 0, 1 => \"bit\"; else => \"something\" }");
        Method foo = generateFunction();
        assertEquals("bit", foo.invoke(null, 0));
        assertEquals("bit", foo.invoke(null, 1));
        assertEquals("something", foo.invoke(null, 2));
    }
}