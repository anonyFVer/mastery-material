package org.apache.dubbo.config.spring.util;

import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.annotation.Service;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertyResolver;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import static java.lang.String.valueOf;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.springframework.core.annotation.AnnotatedElementUtils.getMergedAnnotation;
import static org.springframework.core.annotation.AnnotationAttributes.fromMap;
import static org.springframework.core.annotation.AnnotationUtils.findAnnotation;
import static org.springframework.core.annotation.AnnotationUtils.getAnnotationAttributes;
import static org.springframework.core.annotation.AnnotationUtils.getDefaultValue;
import static org.springframework.util.ClassUtils.getAllInterfacesForClass;
import static org.springframework.util.ClassUtils.resolveClassName;
import static org.springframework.util.CollectionUtils.isEmpty;
import static org.springframework.util.ObjectUtils.containsElement;
import static org.springframework.util.ObjectUtils.nullSafeEquals;
import static org.springframework.util.StringUtils.hasText;
import static org.springframework.util.StringUtils.trimWhitespace;

public class AnnotationUtils {

    @Deprecated
    public static String resolveInterfaceName(Service service, Class<?> defaultInterfaceClass) throws IllegalStateException {
        String interfaceName;
        if (hasText(service.interfaceName())) {
            interfaceName = service.interfaceName();
        } else if (!void.class.equals(service.interfaceClass())) {
            interfaceName = service.interfaceClass().getName();
        } else if (defaultInterfaceClass.isInterface()) {
            interfaceName = defaultInterfaceClass.getName();
        } else {
            throw new IllegalStateException("The @Service undefined interfaceClass or interfaceName, and the type " + defaultInterfaceClass.getName() + " is not a interface.");
        }
        return interfaceName;
    }

    public static String resolveInterfaceName(AnnotationAttributes attributes, Class<?> defaultInterfaceClass) {
        return resolveServiceInterfaceClass(attributes, defaultInterfaceClass).getName();
    }

    public static <T> T getAttribute(AnnotationAttributes attributes, String name) {
        return (T) attributes.get(name);
    }

    public static Class<?> resolveServiceInterfaceClass(AnnotationAttributes attributes, Class<?> defaultInterfaceClass) throws IllegalArgumentException {
        ClassLoader classLoader = defaultInterfaceClass != null ? defaultInterfaceClass.getClassLoader() : Thread.currentThread().getContextClassLoader();
        Class<?> interfaceClass = getAttribute(attributes, "interfaceClass");
        if (void.class.equals(interfaceClass)) {
            interfaceClass = null;
            String interfaceClassName = getAttribute(attributes, "interfaceName");
            if (hasText(interfaceClassName)) {
                if (ClassUtils.isPresent(interfaceClassName, classLoader)) {
                    interfaceClass = resolveClassName(interfaceClassName, classLoader);
                }
            }
        }
        if (interfaceClass == null && defaultInterfaceClass != null) {
            Class<?>[] allInterfaces = getAllInterfacesForClass(defaultInterfaceClass);
            if (allInterfaces.length > 0) {
                interfaceClass = allInterfaces[0];
            }
        }
        Assert.notNull(interfaceClass, "@Service interfaceClass() or interfaceName() or interface class must be present!");
        Assert.isTrue(interfaceClass.isInterface(), "The annotated type must be an interface!");
        return interfaceClass;
    }

    @Deprecated
    public static String resolveInterfaceName(Reference reference, Class<?> defaultInterfaceClass) throws IllegalStateException {
        String interfaceName;
        if (!"".equals(reference.interfaceName())) {
            interfaceName = reference.interfaceName();
        } else if (!void.class.equals(reference.interfaceClass())) {
            interfaceName = reference.interfaceClass().getName();
        } else if (defaultInterfaceClass.isInterface()) {
            interfaceName = defaultInterfaceClass.getName();
        } else {
            throw new IllegalStateException("The @Reference undefined interfaceClass or interfaceName, and the type " + defaultInterfaceClass.getName() + " is not a interface.");
        }
        return interfaceName;
    }

    public static <A extends Annotation> boolean isPresent(Method method, Class<A> annotationClass) {
        Map<ElementType, List<A>> annotationsMap = findAnnotations(method, annotationClass);
        return !annotationsMap.isEmpty();
    }

    public static <A extends Annotation> Map<ElementType, List<A>> findAnnotations(Method method, Class<A> annotationClass) {
        Retention retention = annotationClass.getAnnotation(Retention.class);
        RetentionPolicy retentionPolicy = retention.value();
        if (!RetentionPolicy.RUNTIME.equals(retentionPolicy)) {
            return Collections.emptyMap();
        }
        Map<ElementType, List<A>> annotationsMap = new LinkedHashMap<ElementType, List<A>>();
        Target target = annotationClass.getAnnotation(Target.class);
        ElementType[] elementTypes = target.value();
        for (ElementType elementType : elementTypes) {
            List<A> annotationsList = new LinkedList<A>();
            switch(elementType) {
                case PARAMETER:
                    Annotation[][] parameterAnnotations = method.getParameterAnnotations();
                    for (Annotation[] annotations : parameterAnnotations) {
                        for (Annotation annotation : annotations) {
                            if (annotationClass.equals(annotation.annotationType())) {
                                annotationsList.add((A) annotation);
                            }
                        }
                    }
                    break;
                case METHOD:
                    A annotation = findAnnotation(method, annotationClass);
                    if (annotation != null) {
                        annotationsList.add(annotation);
                    }
                    break;
                case TYPE:
                    Class<?> beanType = method.getDeclaringClass();
                    A annotation2 = findAnnotation(beanType, annotationClass);
                    if (annotation2 != null) {
                        annotationsList.add(annotation2);
                    }
                    break;
            }
            if (!annotationsList.isEmpty()) {
                annotationsMap.put(elementType, annotationsList);
            }
        }
        return unmodifiableMap(annotationsMap);
    }

    @Deprecated
    public static Map<String, Object> getAttributes(Annotation annotation, boolean ignoreDefaultValue, String... ignoreAttributeNames) {
        return getAttributes(annotation, null, ignoreDefaultValue, ignoreAttributeNames);
    }

    public static Map<String, Object> getAttributes(Annotation annotation, PropertyResolver propertyResolver, boolean ignoreDefaultValue, String... ignoreAttributeNames) {
        if (annotation == null) {
            return emptyMap();
        }
        Map<String, Object> attributes = getAnnotationAttributes(annotation);
        Map<String, Object> actualAttributes = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            String attributeName = entry.getKey();
            Object attributeValue = entry.getValue();
            if (ignoreDefaultValue && nullSafeEquals(attributeValue, getDefaultValue(annotation, attributeName))) {
                continue;
            }
            if (attributeValue.getClass().isAnnotation()) {
                continue;
            }
            if (attributeValue.getClass().isArray() && attributeValue.getClass().getComponentType().isAnnotation()) {
                continue;
            }
            actualAttributes.put(attributeName, attributeValue);
        }
        return resolvePlaceholders(actualAttributes, propertyResolver, ignoreAttributeNames);
    }

    public static Map<String, Object> resolvePlaceholders(Map<String, Object> sourceAnnotationAttributes, PropertyResolver propertyResolver, String... ignoreAttributeNames) {
        if (isEmpty(sourceAnnotationAttributes)) {
            return emptyMap();
        }
        Map<String, Object> resolvedAnnotationAttributes = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : sourceAnnotationAttributes.entrySet()) {
            String attributeName = entry.getKey();
            if (containsElement(ignoreAttributeNames, attributeName)) {
                continue;
            }
            Object attributeValue = entry.getValue();
            if (attributeValue instanceof String) {
                attributeValue = resolvePlaceholders(valueOf(attributeValue), propertyResolver);
            } else if (attributeValue instanceof String[]) {
                String[] values = (String[]) attributeValue;
                for (int i = 0; i < values.length; i++) {
                    values[i] = resolvePlaceholders(values[i], propertyResolver);
                }
                attributeValue = values;
            }
            resolvedAnnotationAttributes.put(attributeName, attributeValue);
        }
        return unmodifiableMap(resolvedAnnotationAttributes);
    }

    public static AnnotationAttributes getMergedAttributes(AnnotatedElement annotatedElement, Class<? extends Annotation> annotationType, PropertyResolver propertyResolver, boolean ignoreDefaultValue, String... ignoreAttributeNames) {
        Annotation annotation = getMergedAnnotation(annotatedElement, annotationType);
        return annotation == null ? null : fromMap(getAttributes(annotation, propertyResolver, ignoreDefaultValue, ignoreAttributeNames));
    }

    private static String resolvePlaceholders(String attributeValue, PropertyResolver propertyResolver) {
        String resolvedValue = attributeValue;
        if (propertyResolver != null) {
            resolvedValue = propertyResolver.resolvePlaceholders(resolvedValue);
            resolvedValue = trimWhitespace(resolvedValue);
        }
        return resolvedValue;
    }
}