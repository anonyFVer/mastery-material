package org.apache.dubbo.validation.support.jvalidation;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.bytecode.ClassGenerator;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.validation.MethodValidated;
import org.apache.dubbo.validation.Validator;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.Modifier;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.ArrayMemberValue;
import javassist.bytecode.annotation.BooleanMemberValue;
import javassist.bytecode.annotation.ByteMemberValue;
import javassist.bytecode.annotation.CharMemberValue;
import javassist.bytecode.annotation.ClassMemberValue;
import javassist.bytecode.annotation.DoubleMemberValue;
import javassist.bytecode.annotation.EnumMemberValue;
import javassist.bytecode.annotation.FloatMemberValue;
import javassist.bytecode.annotation.IntegerMemberValue;
import javassist.bytecode.annotation.LongMemberValue;
import javassist.bytecode.annotation.MemberValue;
import javassist.bytecode.annotation.ShortMemberValue;
import javassist.bytecode.annotation.StringMemberValue;
import javax.validation.Constraint;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.validation.groups.Default;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class JValidator implements Validator {

    private static final Logger logger = LoggerFactory.getLogger(JValidator.class);

    private final Class<?> clazz;

    private final Map<String, Class> methodClassMap;

    private final javax.validation.Validator validator;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public JValidator(URL url) {
        this.clazz = ReflectUtils.forName(url.getServiceInterface());
        String jvalidation = url.getParameter("jvalidation");
        ValidatorFactory factory;
        if (jvalidation != null && jvalidation.length() > 0) {
            factory = Validation.byProvider((Class) ReflectUtils.forName(jvalidation)).configure().buildValidatorFactory();
        } else {
            factory = Validation.buildDefaultValidatorFactory();
        }
        this.validator = factory.getValidator();
        this.methodClassMap = new ConcurrentHashMap<String, Class>();
    }

    private static boolean isPrimitives(Class<?> cls) {
        if (cls.isArray()) {
            return isPrimitive(cls.getComponentType());
        }
        return isPrimitive(cls);
    }

    private static boolean isPrimitive(Class<?> cls) {
        return cls.isPrimitive() || cls == String.class || cls == Boolean.class || cls == Character.class || Number.class.isAssignableFrom(cls) || Date.class.isAssignableFrom(cls);
    }

    private static Object getMethodParameterBean(Class<?> clazz, Method method, Object[] args) {
        if (!hasConstraintParameter(method)) {
            return null;
        }
        try {
            String parameterClassName = generateMethodParameterClassName(clazz, method);
            Class<?> parameterClass;
            try {
                parameterClass = (Class<?>) Class.forName(parameterClassName, true, clazz.getClassLoader());
            } catch (ClassNotFoundException e) {
                ClassPool pool = ClassGenerator.getClassPool(clazz.getClassLoader());
                CtClass ctClass = pool.makeClass(parameterClassName);
                ClassFile classFile = ctClass.getClassFile();
                classFile.setVersionToJava5();
                ctClass.addConstructor(CtNewConstructor.defaultConstructor(pool.getCtClass(parameterClassName)));
                Class<?>[] parameterTypes = method.getParameterTypes();
                Annotation[][] parameterAnnotations = method.getParameterAnnotations();
                for (int i = 0; i < parameterTypes.length; i++) {
                    Class<?> type = parameterTypes[i];
                    Annotation[] annotations = parameterAnnotations[i];
                    AnnotationsAttribute attribute = new AnnotationsAttribute(classFile.getConstPool(), AnnotationsAttribute.visibleTag);
                    for (Annotation annotation : annotations) {
                        if (annotation.annotationType().isAnnotationPresent(Constraint.class)) {
                            javassist.bytecode.annotation.Annotation ja = new javassist.bytecode.annotation.Annotation(classFile.getConstPool(), pool.getCtClass(annotation.annotationType().getName()));
                            Method[] members = annotation.annotationType().getMethods();
                            for (Method member : members) {
                                if (Modifier.isPublic(member.getModifiers()) && member.getParameterTypes().length == 0 && member.getDeclaringClass() == annotation.annotationType()) {
                                    Object value = member.invoke(annotation, new Object[0]);
                                    if (null != value) {
                                        MemberValue memberValue = createMemberValue(classFile.getConstPool(), pool.get(member.getReturnType().getName()), value);
                                        ja.addMemberValue(member.getName(), memberValue);
                                    }
                                }
                            }
                            attribute.addAnnotation(ja);
                        }
                    }
                    String fieldName = method.getName() + "Argument" + i;
                    CtField ctField = CtField.make("public " + type.getCanonicalName() + " " + fieldName + ";", pool.getCtClass(parameterClassName));
                    ctField.getFieldInfo().addAttribute(attribute);
                    ctClass.addField(ctField);
                }
                parameterClass = ctClass.toClass(clazz.getClassLoader(), null);
            }
            Object parameterBean = parameterClass.newInstance();
            for (int i = 0; i < args.length; i++) {
                Field field = parameterClass.getField(method.getName() + "Argument" + i);
                field.set(parameterBean, args[i]);
            }
            return parameterBean;
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
            return null;
        }
    }

    private static String generateMethodParameterClassName(Class<?> clazz, Method method) {
        StringBuilder builder = new StringBuilder().append(clazz.getName()).append("_").append(toUpperMethoName(method.getName())).append("Parameter");
        Class<?>[] parameterTypes = method.getParameterTypes();
        for (Class<?> parameterType : parameterTypes) {
            builder.append("_").append(parameterType.getName());
        }
        return builder.toString();
    }

    private static boolean hasConstraintParameter(Method method) {
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        if (parameterAnnotations != null && parameterAnnotations.length > 0) {
            for (Annotation[] annotations : parameterAnnotations) {
                for (Annotation annotation : annotations) {
                    if (annotation.annotationType().isAnnotationPresent(Constraint.class)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static String toUpperMethoName(String methodName) {
        return methodName.substring(0, 1).toUpperCase() + methodName.substring(1);
    }

    private static MemberValue createMemberValue(ConstPool cp, CtClass type, Object value) throws NotFoundException {
        MemberValue memberValue = javassist.bytecode.annotation.Annotation.createMemberValue(cp, type);
        if (memberValue instanceof BooleanMemberValue) {
            ((BooleanMemberValue) memberValue).setValue((Boolean) value);
        } else if (memberValue instanceof ByteMemberValue) {
            ((ByteMemberValue) memberValue).setValue((Byte) value);
        } else if (memberValue instanceof CharMemberValue) {
            ((CharMemberValue) memberValue).setValue((Character) value);
        } else if (memberValue instanceof ShortMemberValue) {
            ((ShortMemberValue) memberValue).setValue((Short) value);
        } else if (memberValue instanceof IntegerMemberValue) {
            ((IntegerMemberValue) memberValue).setValue((Integer) value);
        } else if (memberValue instanceof LongMemberValue) {
            ((LongMemberValue) memberValue).setValue((Long) value);
        } else if (memberValue instanceof FloatMemberValue) {
            ((FloatMemberValue) memberValue).setValue((Float) value);
        } else if (memberValue instanceof DoubleMemberValue) {
            ((DoubleMemberValue) memberValue).setValue((Double) value);
        } else if (memberValue instanceof ClassMemberValue) {
            ((ClassMemberValue) memberValue).setValue(((Class<?>) value).getName());
        } else if (memberValue instanceof StringMemberValue) {
            ((StringMemberValue) memberValue).setValue((String) value);
        } else if (memberValue instanceof EnumMemberValue) {
            ((EnumMemberValue) memberValue).setValue(((Enum<?>) value).name());
        } else if (memberValue instanceof ArrayMemberValue) {
            CtClass arrayType = type.getComponentType();
            int len = Array.getLength(value);
            MemberValue[] members = new MemberValue[len];
            for (int i = 0; i < len; i++) {
                members[i] = createMemberValue(cp, arrayType, Array.get(value, i));
            }
            ((ArrayMemberValue) memberValue).setValue(members);
        }
        return memberValue;
    }

    @Override
    public void validate(String methodName, Class<?>[] parameterTypes, Object[] arguments) throws Exception {
        List<Class<?>> groups = new ArrayList<Class<?>>();
        Class<?> methodClass = methodClass(methodName);
        if (methodClass != null) {
            groups.add(methodClass);
        }
        Set<ConstraintViolation<?>> violations = new HashSet<ConstraintViolation<?>>();
        Method method = clazz.getMethod(methodName, parameterTypes);
        Class<?>[] methodClasses = null;
        if (method.isAnnotationPresent(MethodValidated.class)) {
            methodClasses = method.getAnnotation(MethodValidated.class).value();
            groups.addAll(Arrays.asList(methodClasses));
        }
        groups.add(0, Default.class);
        groups.add(1, clazz);
        Class<?>[] classgroups = groups.toArray(new Class[0]);
        Object parameterBean = getMethodParameterBean(clazz, method, arguments);
        if (parameterBean != null) {
            violations.addAll(validator.validate(parameterBean, classgroups));
        }
        for (Object arg : arguments) {
            validate(violations, arg, classgroups);
        }
        if (!violations.isEmpty()) {
            logger.error("Failed to validate service: " + clazz.getName() + ", method: " + methodName + ", cause: " + violations);
            throw new ConstraintViolationException("Failed to validate service: " + clazz.getName() + ", method: " + methodName + ", cause: " + violations, violations);
        }
    }

    private Class methodClass(String methodName) {
        Class<?> methodClass = null;
        String methodClassName = clazz.getName() + "$" + toUpperMethoName(methodName);
        Class cached = methodClassMap.get(methodClassName);
        if (cached != null) {
            return cached == clazz ? null : cached;
        }
        try {
            methodClass = Class.forName(methodClassName, false, Thread.currentThread().getContextClassLoader());
            methodClassMap.put(methodClassName, methodClass);
        } catch (ClassNotFoundException e) {
            methodClassMap.put(methodClassName, clazz);
        }
        return methodClass;
    }

    private void validate(Set<ConstraintViolation<?>> violations, Object arg, Class<?>... groups) {
        if (arg != null && !isPrimitives(arg.getClass())) {
            if (Object[].class.isInstance(arg)) {
                for (Object item : (Object[]) arg) {
                    validate(violations, item, groups);
                }
            } else if (Collection.class.isInstance(arg)) {
                for (Object item : (Collection<?>) arg) {
                    validate(violations, item, groups);
                }
            } else if (Map.class.isInstance(arg)) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) arg).entrySet()) {
                    validate(violations, entry.getKey(), groups);
                    validate(violations, entry.getValue(), groups);
                }
            } else {
                violations.addAll(validator.validate(arg, groups));
            }
        }
    }
}