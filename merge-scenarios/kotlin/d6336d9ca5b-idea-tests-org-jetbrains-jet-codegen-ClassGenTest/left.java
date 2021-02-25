package org.jetbrains.jet.codegen;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

/**
 * @author yole
 */
public class ClassGenTest extends CodegenTestCase {
    public void testPSVMClass() throws Exception {
        loadFile("classes/simpleClass.jet");

        final Class aClass = loadClass("SimpleClass", generateClassesInFile());
        final Method[] methods = aClass.getDeclaredMethods();
        assertEquals(1, methods.length);
    }

    public void testArrayListInheritance() throws Exception {
        loadFile("classes/inheritingFromArrayList.jet");

        final Class aClass = loadClass("Foo", generateClassesInFile());
        checkInterface(aClass, List.class);
    }

    public void testInheritanceAndDelegation_DelegatingDefaultConstructorProperties() throws Exception {
        blackBoxFile("classes/inheritance.jet");
    }

    public void testFunDelegation() throws Exception {
        blackBoxFile("classes/funDelegation.jet");
    }

    public void testPropertyDelegation() throws Exception {
        blackBoxFile("classes/propertyDelegation.jet");
    }

    public void testDiamondInheritance() throws Exception {
        blackBoxFile("classes/diamondInheritance.jet");
    }

    public void testRightHandOverride() throws Exception {
        blackBoxFile("classes/rightHandOverride.jet");
    }

    private static void checkInterface(Class aClass, Class ifs) {
        for (Class anInterface : aClass.getInterfaces()) {
            if (anInterface == ifs) return;
        }
        fail(aClass.getName() + " must have " + ifs.getName() + " in its interfaces");
    }

    public void testNewInstanceExplicitConstructor() throws Exception {
        loadFile("classes/newInstanceDefaultConstructor.jet");
        System.out.println(generateToText());
        final Method method = generateFunction("test");
        final Integer returnValue = (Integer) method.invoke(null);
        assertEquals(610, returnValue.intValue());
    }

    public void testInnerClass() throws Exception {
        blackBoxFile("classes/innerClass.jet");
    }

    public void testInheritedInnerClass() throws Exception {
        blackBoxFile("classes/inheritedInnerClass.jet");
    }

    public void testInitializerBlock() throws Exception {
        blackBoxFile("classes/initializerBlock.jet");
    }

    public void testAbstractMethod() throws Exception {
        loadText("class Foo { abstract fun x(): String; fun y(): Int = 0 }");

        final ClassFileFactory codegens = generateClassesInFile();
        final Class aClass = loadClass("Foo", codegens);
        assertNotNull(aClass.getMethod("x"));
        final Class implClass = loadClass("Foo$$Impl", codegens);
        assertNull(findMethodByName(implClass, "x"));
        assertNotNull(findMethodByName(implClass, "y"));
    }

    public void testInheritedMethod() throws Exception {
        blackBoxFile("classes/inheritedMethod.jet");
    }

    public void testInitializerBlockDImpl() throws Exception {
        blackBoxFile("classes/initializerBlockDImpl.jet");
    }

    public void testPropertyInInitializer() throws Exception {
        blackBoxFile("classes/propertyInInitializer.jet");
    }

    public void testOuterThis() throws Exception {
        blackBoxFile("classes/outerThis.jet");
    }

    public void testSecondaryConstructors() throws Exception {
        blackBoxFile("classes/secondaryConstructors.jet");
    }

    public void testExceptionConstructor() throws Exception {
        blackBoxFile("classes/exceptionConstructor.jet");
    }

    public void testSimpleBox() throws Exception {
        blackBoxFile("classes/simpleBox.jet");
    }

    public void testAbstractClass() throws Exception {
        loadText("abstract class SimpleClass() { }");

        final Class aClass = loadAllClasses(generateClassesInFile()).get("SimpleClass$$Impl");
        assertTrue((aClass.getModifiers() & Modifier.ABSTRACT) != 0);
    }

    public void testClassObject() throws Exception {
        blackBoxFile("classes/classObject.jet");
    }

    public void testClassObjectMethod() throws Exception {
        blackBoxFile("classes/classObjectMethod.jet");
    }

    public void testClassObjectInterface() throws Exception {
        loadFile("classes/classObjectInterface.jet");
        final Method method = generateFunction();
        Object result = method.invoke(null);
        assertInstanceOf(result, Runnable.class);
    }

    public void testOverloadBinaryOperator() throws Exception {
        blackBoxFile("classes/overloadBinaryOperator.jet");
    }

    public void testOverloadUnaryOperator() throws Exception {
        blackBoxFile("classes/overloadUnaryOperator.jet");
    }

    public void testOverloadPlusAssign() throws Exception {
        blackBoxFile("classes/overloadPlusAssign.jet");
    }

    public void testOverloadPlusAssignReturn() throws Exception {
        blackBoxFile("classes/overloadPlusAssignReturn.jet");
    }

    public void testOverloadPlusToPlusAssign() throws Exception {
        blackBoxFile("classes/overloadPlusToPlusAssign.jet");
    }

    public void testEnumClass() throws Exception {
        loadText("enum class Direction { NORTH; SOUTH; EAST; WEST }");
        final Class direction = loadAllClasses(generateClassesInFile()).get("Direction");
        final Field north = direction.getField("NORTH");
        assertEquals(direction, north.getType());
        assertInstanceOf(north.get(null), direction);
    }

    public void testEnumConstantConstructors() throws Exception {
        loadText("enum class Color(val rgb: Int) { RED: Color(0xFF0000); GREEN: Color(0x00FF00); }");
        final Class colorClass = loadAllClasses(generateClassesInFile()).get("Color");
        final Field redField = colorClass.getField("RED");
        final Object redValue = redField.get(null);
        final Method rgbMethod = colorClass.getMethod("getRgb");
        assertEquals(0xFF0000, rgbMethod.invoke(redValue));
    }

    public void testKt249() throws Exception {
        blackBoxFile("regressions/kt249.jet");
    }

    public void testKt48 () throws Exception {
        blackBoxFile("regressions/kt48.jet");
        System.out.println(generateToText());
    }
}