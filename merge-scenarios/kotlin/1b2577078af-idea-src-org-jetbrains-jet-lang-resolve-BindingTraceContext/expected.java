package org.jetbrains.jet.lang.resolve;

import com.google.common.collect.Maps;
import com.intellij.psi.PsiElement;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.jet.codegen.ClassCodegen;
import org.jetbrains.jet.lang.psi.*;
import org.jetbrains.jet.lang.types.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author abreslav
 */
public class BindingTraceContext implements BindingContext, BindingTrace {
    private final Map<JetExpression, JetType> expressionTypes = new HashMap<JetExpression, JetType>();
    private final Map<JetReferenceExpression, DeclarationDescriptor> resolutionResults = new HashMap<JetReferenceExpression, DeclarationDescriptor>();
    private final Map<JetReferenceExpression, PsiElement> labelResolutionResults = new HashMap<JetReferenceExpression, PsiElement>();
    private final Map<JetTypeReference, JetType> types = new HashMap<JetTypeReference, JetType>();
    private final Map<DeclarationDescriptor, PsiElement> descriptorToDeclarations = new HashMap<DeclarationDescriptor, PsiElement>();
    private final Map<PsiElement, DeclarationDescriptor> declarationsToDescriptors = new HashMap<PsiElement, DeclarationDescriptor>();
    private final Map<PsiElement, ConstructorDescriptor> constructorDeclarationsToDescriptors = new HashMap<PsiElement, ConstructorDescriptor>();
    private final Map<PsiElement, PropertyDescriptor> propertyDeclarationsToDescriptors = Maps.newHashMap();
    private final Set<JetFunctionLiteralExpression> blocks = new HashSet<JetFunctionLiteralExpression>();
    private final Set<JetElement> statements = new HashSet<JetElement>();
    private final Set<PropertyDescriptor> backingFieldRequired = new HashSet<PropertyDescriptor>();

    private JetScope toplevelScope;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void recordExpressionType(@NotNull JetExpression expression, @NotNull JetType type) {
        expressionTypes.put(expression, type);
    }

    @Override
    public void recordReferenceResolution(@NotNull JetReferenceExpression expression, @NotNull DeclarationDescriptor descriptor) {
        resolutionResults.put(expression, descriptor);
    }

    @Override
    public void recordLabelResolution(@NotNull JetReferenceExpression expression, @NotNull PsiElement element) {
        labelResolutionResults.put(expression, element);
    }

    @Override
    public void removeReferenceResolution(@NotNull JetReferenceExpression referenceExpression) {
        resolutionResults.remove(referenceExpression);
    }

    @Override
    public void recordTypeResolution(@NotNull JetTypeReference typeReference, @NotNull JetType type) {
        types.put(typeReference, type);
    }

    @Override
    public void recordDeclarationResolution(@NotNull PsiElement declaration, @NotNull DeclarationDescriptor descriptor) {
        safePut(descriptorToDeclarations, descriptor.getOriginal(), declaration);
        descriptor.accept(new DeclarationDescriptorVisitor<Void, PsiElement>() {
                    @Override
                    public Void visitConstructorDescriptor(ConstructorDescriptor constructorDescriptor, PsiElement declaration) {
                        safePut(constructorDeclarationsToDescriptors, declaration, constructorDescriptor);
                        return null;
                    }



                    @Override
                    public Void visitDeclarationDescriptor(DeclarationDescriptor descriptor, PsiElement declaration) {
                        safePut(declarationsToDescriptors, declaration, descriptor.getOriginal());
                        return null;
                    }
                }, declaration);
    }

    private <K, V> void safePut(Map<K, V> map, K key, V value) {
        V oldValue = map.put(key, value);
        // TODO:
        assert oldValue == null || oldValue == value : key + ": " + oldValue + " and " + value;
    }

    @Override
    public void requireBackingField(@NotNull PropertyDescriptor propertyDescriptor) {
        backingFieldRequired.add(propertyDescriptor);
    }

    @Override
    public void recordBlock(JetFunctionLiteralExpression expression) {
        blocks.add(expression);
    }

    @Override
    public void recordStatement(@NotNull JetElement statement) {
        statements.add(statement);
    }

    @Override
    public void removeStatementRecord(@NotNull JetElement statement) {
        statements.remove(statement);
    }

    public void setToplevelScope(JetScope toplevelScope) {
        this.toplevelScope = toplevelScope;
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    public NamespaceDescriptor getNamespaceDescriptor(JetNamespace declaration) {
        return (NamespaceDescriptor) declarationsToDescriptors.get(declaration);
    }

    @Override
    public ClassDescriptor getClassDescriptor(JetClass declaration) {
        return (ClassDescriptor) declarationsToDescriptors.get(declaration);
    }

    @Override
    public TypeParameterDescriptor getTypeParameterDescriptor(JetTypeParameter declaration) {
        return (TypeParameterDescriptor) declarationsToDescriptors.get(declaration);
    }

    @Override
    public FunctionDescriptor getFunctionDescriptor(JetFunction declaration) {
        return (FunctionDescriptor) declarationsToDescriptors.get(declaration);
    }

    @Override
    public VariableDescriptor getVariableDescriptor(JetProperty declaration) {
        return (VariableDescriptor) declarationsToDescriptors.get(declaration);
    }

    @Override
    public VariableDescriptor getParameterDescriptor(JetParameter declaration) {
        return (VariableDescriptor) declarationsToDescriptors.get(declaration);
    }

    @Override
    public JetType resolveTypeReference(JetTypeReference typeReference) {
        return types.get(typeReference);
    }

    @Override
    public JetType getExpressionType(JetExpression expression) {
        return expressionTypes.get(expression);
    }

    @Nullable
    @Override
    public ConstructorDescriptor getConstructorDescriptor(@NotNull JetElement declaration) {
        return constructorDeclarationsToDescriptors.get(declaration);
    }

    @Override
    public DeclarationDescriptor resolveReferenceExpression(JetReferenceExpression referenceExpression) {
        return resolutionResults.get(referenceExpression);
    }

    @Override
    public JetScope getTopLevelScope() {
        return toplevelScope;
    }

    @Override
    public PsiElement resolveToDeclarationPsiElement(JetReferenceExpression referenceExpression) {
        DeclarationDescriptor declarationDescriptor = resolveReferenceExpression(referenceExpression);
        if (declarationDescriptor == null) {
            return labelResolutionResults.get(referenceExpression);
        }
        return descriptorToDeclarations.get(declarationDescriptor.getOriginal());
    }

    @Override
    public PsiElement getDeclarationPsiElement(@NotNull DeclarationDescriptor descriptor) {
        return descriptorToDeclarations.get(descriptor.getOriginal());
    }

    @Override
    public boolean isBlock(JetFunctionLiteralExpression expression) {
        return !expression.hasParameterSpecification() && blocks.contains(expression);
    }

    @Override
    public boolean isStatement(@NotNull JetExpression expression) {
        return statements.contains(expression);
    }

    @Override
    public boolean hasBackingField(@NotNull PropertyDescriptor propertyDescriptor) {
        if (propertyDescriptor.getModifiers().isAbstract()) return false;
        PropertyGetterDescriptor getter = propertyDescriptor.getGetter();
        PropertySetterDescriptor setter = propertyDescriptor.getSetter();
        if (getter == null) {
            return true;
        }
        else if (propertyDescriptor.isVar() && setter == null) {
            return true;
        }
        else if (setter != null && !setter.hasBody()) {
            return true;
        }
        else if (!getter.hasBody()) {
            return true;
        }
        return backingFieldRequired.contains(propertyDescriptor);
    }

    public ConstructorDescriptor resolveSuperConstructor(JetDelegatorToSuperCall superCall, ClassCodegen classCodegen) {
        JetTypeReference typeReference = superCall.getTypeReference();
        if (typeReference == null) return null;

        JetTypeElement typeElement = typeReference.getTypeElement();
        if (!(typeElement instanceof JetUserType)) return null;

        DeclarationDescriptor descriptor = resolveReferenceExpression(((JetUserType) typeElement).getReferenceExpression());
        return descriptor instanceof ConstructorDescriptor ? (ConstructorDescriptor) descriptor : null;
    }
}