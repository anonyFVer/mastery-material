package org.jetbrains.jet.codegen;

import jet.typeinfo.TypeInfo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * @author yole
 */
public class ControlStructuresTest extends CodegenTestCase {
    @Override
    protected String getPrefix() {
        return "controlStructures";
    }

    public void testIf() throws Exception {
        loadFile();

        System.out.println(generateToText());
        final Method main = generateFunction();
        assertEquals(15, main.invoke(null, true));
        assertEquals(20, main.invoke(null, false));
    }

    public void testSingleBranchIf() throws Exception {
        loadFile();

        System.out.println(generateToText());
        final Method main = generateFunction();
        assertEquals(15, main.invoke(null, true));
        assertEquals(20, main.invoke(null, false));
    }

    public void testWhile() throws Exception {
        factorialTest("controlStructures/while.jet");
    }

    public void testDoWhile() throws Exception {
        factorialTest("controlStructures/doWhile.jet");
    }

    public void testBreak() throws Exception {
        factorialTest("controlStructures/break.jet");
    }

    private void factorialTest(final String name) throws IllegalAccessException, InvocationTargetException {
        loadFile(name);

        System.out.println(generateToText());
        final Method main = generateFunction();
        assertEquals(6, main.invoke(null, 3));
        assertEquals(120, main.invoke(null, 5));
    }

    public void testContinue() throws Exception {
        loadFile();
        System.out.println(generateToText());
        final Method main = generateFunction();
        assertEquals(3, main.invoke(null, 4));
        assertEquals(7, main.invoke(null, 5));
    }

    public void testIfNoElse() throws Exception {
        loadFile();
        System.out.println(generateToText());
        final Method main = generateFunction();
        assertEquals(5, main.invoke(null, 5, true));
        assertEquals(10, main.invoke(null, 5, false));
    }

    public void testCondJumpOnStack() throws Exception {
        loadText("import java.lang.Boolean as jlBoolean; fun foo(a: String): Int = if (jlBoolean.parseBoolean(a)) 5 else 10");
        final Method main = generateFunction();
        assertEquals(5, main.invoke(null, "true"));
        assertEquals(10, main.invoke(null, "false"));
    }

    public void testFor() throws Exception {
        loadFile();
        System.out.println(generateToText());
        final Method main = generateFunction();
        List<String> args = Arrays.asList("IntelliJ", " ", "IDEA");
        assertEquals("IntelliJ IDEA", main.invoke(null, args));
    }

    public void testForInArray() throws Exception {
        loadFile();
        System.out.println(generateToText());
        final Method main = generateFunction();
        String[] args = new String[] { "IntelliJ", " ", "IDEA" };
        assertEquals("IntelliJ IDEA", main.invoke(null, new Object[] { args }));
    }

    public void testForInRange() throws Exception {
        loadText("fun foo(sb: StringBuilder) { for(x in 1..4) sb.append(x) }");
        final Method main = generateFunction();
        StringBuilder stringBuilder = new StringBuilder();
        main.invoke(null, stringBuilder);
        assertEquals("1234", stringBuilder.toString());
    }

    public void testThrowCheckedException() throws Exception {
        loadText("fun foo() { throw Exception(); }");
        final Method main = generateFunction();
        boolean caught = false;
        try {
            main.invoke(null);
        } catch (InvocationTargetException e) {
            if (e.getTargetException().getClass() == Exception.class) {
                caught = true;
            }
        }
        assertTrue(caught);
    }

    public void testTryCatch() throws Exception {
        loadFile();
        System.out.println(generateToText());
        final Method main = generateFunction();
        assertEquals("no message", main.invoke(null, "0"));
        assertEquals("For input string: \"a\"", main.invoke(null, "a"));
    }

    public void testTryFinally() throws Exception {
        loadFile();
        System.out.println(generateToText());
        final Method main = generateFunction();
        StringBuilder sb = new StringBuilder();
        main.invoke(null, sb, "9");
        assertEquals("foo9bar", sb.toString());
        sb = new StringBuilder();
        boolean caught = false;
        try {
            main.invoke(null, sb, "x");
        }
        catch(InvocationTargetException e) {
            caught = e.getTargetException() instanceof NumberFormatException;
        }
        assertTrue(caught);
        assertEquals("foobar", sb.toString());
    }

    public void testForUserType() throws Exception {
        blackBoxFile("controlStructures/forUserType.jet");
    }

    public void testForIntArray() throws Exception {
        blackBoxFile("controlStructures/forIntArray.jet");
    }

    public void testForPrimitiveIntArray() throws Exception {
        blackBoxFile("controlStructures/forPrimitiveIntArray.jet");
    }

    public void testForNullableIntArray() throws Exception {
        blackBoxFile("controlStructures/forNullableIntArray.jet");
    }

    public void testKt237() throws Exception {
        blackBoxFile("regressions/kt237.jet");
    }

    public void testCompareToNull() throws Exception {
        loadText("fun foo(a: String?, b: String?): Boolean = a == null && b !== null && null == a && null !== b");
        String text = generateToText();
        assertTrue(!text.contains("java/lang/Object.equals"));
        System.out.println(text);
        final Method main = generateFunction();
        assertEquals(true, main.invoke(null, null, "lala"));
        assertEquals(false, main.invoke(null, null, null));
    }

    public void testCompareToNonnullableEq() throws Exception {
        loadText("fun foo(a: String?, b: String): Boolean = a == b || b == a");
        String text = generateToText();
        System.out.println(text);
        final Method main = generateFunction();
        assertEquals(false, main.invoke(null, null, "lala"));
        assertEquals(true, main.invoke(null, "papa", "papa"));
    }

    public void testCompareToNonnullableNotEq() throws Exception {
        loadText("fun foo(a: String?, b: String): Boolean = a != b");
        String text = generateToText();
        System.out.println(text);
        assertTrue(text.contains("IXOR"));
        final Method main = generateFunction();
        assertEquals(true, main.invoke(null, null, "lala"));
        assertEquals(false, main.invoke(null, "papa", "papa"));
    }

    public void testKt299() throws Exception {
        blackBoxFile("regressions/kt299.jet");
    }
}