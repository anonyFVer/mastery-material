package org.jetbrains.jet.lang.resolve.java;

import com.google.common.collect.Lists;
import com.intellij.psi.*;
import org.jetbrains.jet.rt.signature.JetSignatureReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.jet.lang.descriptors.annotations.AnnotationDescriptor;
import org.jetbrains.jet.lang.descriptors.ClassDescriptor;
import org.jetbrains.jet.lang.descriptors.TypeParameterDescriptor;
import org.jetbrains.jet.lang.types.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author abreslav
 */
public class JavaTypeTransformer {

    private final JavaSemanticServices javaSemanticServices;
    private final JavaDescriptorResolver resolver;
    private final JetStandardLibrary standardLibrary;
    private Map<String, JetType> primitiveTypesMap;
    private Map<String, JetType> classTypesMap;
    private Map<String, ClassDescriptor> classDescriptorMap;

    public JavaTypeTransformer(JavaSemanticServices javaSemanticServices, JetStandardLibrary standardLibrary, JavaDescriptorResolver resolver) {
        this.javaSemanticServices = javaSemanticServices;
        this.resolver = resolver;
        this.standardLibrary = standardLibrary;
    }

    @NotNull
    public TypeProjection transformToTypeProjection(@NotNull final PsiType javaType, @NotNull final TypeParameterDescriptor typeParameterDescriptor) {
        TypeProjection result = javaType.accept(new PsiTypeVisitor<TypeProjection>() {

            @Override
            public TypeProjection visitCapturedWildcardType(PsiCapturedWildcardType capturedWildcardType) {
                throw new UnsupportedOperationException(); // TODO
            }

            @Override
            public TypeProjection visitWildcardType(PsiWildcardType wildcardType) {
                if (!wildcardType.isBounded()) {
                    return TypeUtils.makeStarProjection(typeParameterDescriptor);
                }
                Variance variance = wildcardType.isExtends() ? Variance.OUT_VARIANCE : Variance.IN_VARIANCE;

                PsiType bound = wildcardType.getBound();
                assert bound != null;
                return new TypeProjection(variance, transformToType(bound));
            }

            @Override
            public TypeProjection visitType(PsiType type) {
                return new TypeProjection(transformToType(type));
            }
        });
        return result;
    }

    @NotNull
    public JetType transformToType(@NotNull String kotlinSignature, TypeVariableResolver typeVariableResolver) {
        final JetType[] r = new JetType[1];
        JetTypeJetSignatureReader reader = new JetTypeJetSignatureReader(javaSemanticServices, standardLibrary, typeVariableResolver) {
            @Override
            protected void done(@NotNull JetType jetType) {
                r[0] = jetType;
            }
        };
        new JetSignatureReader(kotlinSignature).acceptType(reader);
        return r[0];
    }

    @NotNull
    public JetType transformToType(@NotNull PsiType javaType) {
        return javaType.accept(new PsiTypeVisitor<JetType>() {
            @Override
            public JetType visitClassType(PsiClassType classType) {
                PsiClassType.ClassResolveResult classResolveResult = classType.resolveGenerics();
                PsiClass psiClass = classResolveResult.getElement();
                if (psiClass == null) {
                    return ErrorUtils.createErrorType("Unresolved java class: " + classType.getPresentableText());
                }

                if (psiClass instanceof PsiTypeParameter) {
                    PsiTypeParameter typeParameter = (PsiTypeParameter) psiClass;
                    TypeParameterDescriptor typeParameterDescriptor = resolver.resolveTypeParameter(typeParameter);
//                    return TypeUtils.makeNullable(typeParameterDescriptor.getDefaultType());
                    return typeParameterDescriptor.getDefaultType();
                }
                else {
                    JetType jetAnalog = getClassTypesMap().get(psiClass.getQualifiedName());
                    if (jetAnalog != null) {
                        return jetAnalog;
                    }

                    ClassDescriptor descriptor = resolver.resolveClass(psiClass);
                    if (descriptor == null) {
                        return ErrorUtils.createErrorType("Unresolve java class: " + classType.getPresentableText());
                    }

                    List<TypeProjection> arguments = Lists.newArrayList();
                    if (classType.isRaw()) {
                        List<TypeParameterDescriptor> parameters = descriptor.getTypeConstructor().getParameters();
                        for (TypeParameterDescriptor parameter : parameters) {
                            arguments.add(TypeUtils.makeStarProjection(parameter));
                        }
                    } else {
                        PsiType[] psiArguments = classType.getParameters();
                        for (int i = 0, psiArgumentsLength = psiArguments.length; i < psiArgumentsLength; i++) {
                            PsiType psiArgument = psiArguments[i];
                            TypeParameterDescriptor typeParameterDescriptor = descriptor.getTypeConstructor().getParameters().get(i);
                            arguments.add(transformToTypeProjection(psiArgument, typeParameterDescriptor));
                        }
                    }
                    return new JetTypeImpl(
                            Collections.<AnnotationDescriptor>emptyList(),
                            descriptor.getTypeConstructor(),
                            true,
                            arguments,
                            descriptor.getMemberScope(arguments));
                }
            }

            @Override
            public JetType visitPrimitiveType(PsiPrimitiveType primitiveType) {
                String canonicalText = primitiveType.getCanonicalText();
                JetType type = getPrimitiveTypesMap().get(canonicalText);
                assert type != null : canonicalText;
                return type;
            }

            @Override
            public JetType visitArrayType(PsiArrayType arrayType) {
                PsiType componentType = arrayType.getComponentType();
                if(componentType instanceof PsiPrimitiveType) {
                    JetType jetType = getPrimitiveTypesMap().get("[" + componentType.getCanonicalText());
                    if(jetType != null)
                        return TypeUtils.makeNullable(jetType);
                }

                JetType type = transformToType(componentType);
                return TypeUtils.makeNullable(standardLibrary.getArrayType(type));
            }

            @Override
            public JetType visitType(PsiType type) {
                throw new UnsupportedOperationException("Unsupported type: " + type.getPresentableText()); // TODO
            }
        });
    }

    public Map<String, JetType> getPrimitiveTypesMap() {
        if (primitiveTypesMap == null) {
            primitiveTypesMap = new HashMap<String, JetType>();
            for (JvmPrimitiveType jvmPrimitiveType : JvmPrimitiveType.values()) {
                PrimitiveType primitiveType = jvmPrimitiveType.getPrimitiveType();
                primitiveTypesMap.put(jvmPrimitiveType.getName(), standardLibrary.getPrimitiveJetType(primitiveType));
                primitiveTypesMap.put("[" + jvmPrimitiveType.getName(), standardLibrary.getPrimitiveArrayJetType(primitiveType));
                primitiveTypesMap.put(jvmPrimitiveType.getWrapper().getFqName(), standardLibrary.getNullablePrimitiveJetType(primitiveType));
            }
            primitiveTypesMap.put("void", JetStandardClasses.getUnitType());
        }
        return primitiveTypesMap;
    }

    public Map<String, JetType> getClassTypesMap() {
        if (classTypesMap == null) {
            classTypesMap = new HashMap<String, JetType>();
            for (JvmPrimitiveType jvmPrimitiveType : JvmPrimitiveType.values()) {
                PrimitiveType primitiveType = jvmPrimitiveType.getPrimitiveType();
                classTypesMap.put(jvmPrimitiveType.getWrapper().getFqName(), standardLibrary.getNullablePrimitiveJetType(primitiveType));
            }
            classTypesMap.put("java.lang.Object", JetStandardClasses.getNullableAnyType());
            classTypesMap.put("java.lang.String", standardLibrary.getNullableStringType());
        }
        return classTypesMap;
    }
    
    public Map<String, ClassDescriptor> getPrimitiveWrappersClassDescriptorMap() {
        if (classDescriptorMap == null) {
            classDescriptorMap = new HashMap<String, ClassDescriptor>();
            for (JvmPrimitiveType jvmPrimitiveType : JvmPrimitiveType.values()) {
                PrimitiveType primitiveType = jvmPrimitiveType.getPrimitiveType();
                classDescriptorMap.put(jvmPrimitiveType.getWrapper().getFqName(), standardLibrary.getPrimitiveClassDescriptor(primitiveType));
            }
            //classDescriptorMap.put("java.lang.Object", standardLibrary.get
            classDescriptorMap.put("java.lang.String", standardLibrary.getString());
        }
        return classDescriptorMap;
    }
}