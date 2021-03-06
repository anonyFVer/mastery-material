/*
 * Copyright 2010-2013 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jetbrains.jet.lang.resolve;

import com.google.common.collect.ImmutableMap;
import com.intellij.psi.PsiElement;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.jet.lang.descriptors.*;
import org.jetbrains.jet.lang.descriptors.annotations.AnnotationDescriptor;
import org.jetbrains.jet.lang.diagnostics.Diagnostic;
import org.jetbrains.jet.lang.psi.*;
import org.jetbrains.jet.lang.resolve.calls.autocasts.DataFlowInfo;
import org.jetbrains.jet.lang.resolve.calls.inference.ConstraintSystemCompleter;
import org.jetbrains.jet.lang.resolve.calls.model.ResolvedCall;
import org.jetbrains.jet.lang.resolve.constants.CompileTimeConstant;
import org.jetbrains.jet.lang.resolve.name.FqName;
import org.jetbrains.jet.lang.resolve.scopes.JetScope;
import org.jetbrains.jet.lang.types.DeferredType;
import org.jetbrains.jet.lang.types.JetType;
import org.jetbrains.jet.lang.types.expressions.CaptureKind;
import org.jetbrains.jet.util.Box;
import org.jetbrains.jet.util.slicedmap.*;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.jetbrains.jet.util.slicedmap.RewritePolicy.DO_NOTHING;

public interface BindingContext {
    BindingContext EMPTY = new BindingContext() {
        @Override
        public Collection<Diagnostic> getDiagnostics() {
            return Collections.emptyList();
        }

        @Override
        public <K, V> V get(ReadOnlySlice<K, V> slice, K key) {
            return null;
        }

        @NotNull
        @Override
        public <K, V> Collection<K> getKeys(WritableSlice<K, V> slice) {
            return Collections.emptyList();
        }

        @NotNull
        @TestOnly
        @Override
        public <K, V> ImmutableMap<K, V> getSliceContents(@NotNull ReadOnlySlice<K, V> slice) {
            return ImmutableMap.of();
        }
    };

    WritableSlice<AnnotationDescriptor, JetAnnotationEntry> ANNOTATION_DESCRIPTOR_TO_PSI_ELEMENT = Slices.createSimpleSlice();
    WritableSlice<JetAnnotationEntry, AnnotationDescriptor> ANNOTATION =
            Slices.<JetAnnotationEntry, AnnotationDescriptor>sliceBuilder().setOpposite(ANNOTATION_DESCRIPTOR_TO_PSI_ELEMENT).build();

    WritableSlice<JetExpression, CompileTimeConstant<?>> COMPILE_TIME_VALUE = Slices.createSimpleSlice();
    WritableSlice<JetTypeReference, JetType> TYPE = Slices.createSimpleSlice();
    WritableSlice<JetExpression, JetType> EXPRESSION_TYPE = new BasicWritableSlice<JetExpression, JetType>(DO_NOTHING);
    WritableSlice<JetExpression, JetType> EXPECTED_EXPRESSION_TYPE = new BasicWritableSlice<JetExpression, JetType>(DO_NOTHING);
    WritableSlice<JetExpression, DataFlowInfo> EXPRESSION_DATA_FLOW_INFO = new BasicWritableSlice<JetExpression, DataFlowInfo>(DO_NOTHING);
    WritableSlice<JetExpression, DataFlowInfo> DATAFLOW_INFO_AFTER_CONDITION = Slices.createSimpleSlice();

    WritableSlice<JetReferenceExpression, DeclarationDescriptor> REFERENCE_TARGET =
            new BasicWritableSlice<JetReferenceExpression, DeclarationDescriptor>(DO_NOTHING);
    WritableSlice<JetElement, ResolvedCall<? extends CallableDescriptor>> RESOLVED_CALL =
            new BasicWritableSlice<JetElement, ResolvedCall<? extends CallableDescriptor>>(DO_NOTHING);
    WritableSlice<JetElement, ConstraintSystemCompleter> CONSTRAINT_SYSTEM_COMPLETER = new BasicWritableSlice<JetElement, ConstraintSystemCompleter>(DO_NOTHING);
    WritableSlice<JetElement, Call> CALL = new BasicWritableSlice<JetElement, Call>(DO_NOTHING);

    WritableSlice<JetReferenceExpression, Collection<? extends DeclarationDescriptor>> AMBIGUOUS_REFERENCE_TARGET =
            new BasicWritableSlice<JetReferenceExpression, Collection<? extends DeclarationDescriptor>>(DO_NOTHING);

    WritableSlice<JetExpression, DelegatingBindingTrace> TRACE_DELTAS_CACHE = Slices.createSimpleSlice();

    WritableSlice<JetExpression, ResolvedCall<FunctionDescriptor>> LOOP_RANGE_ITERATOR_RESOLVED_CALL = Slices.createSimpleSlice();
    WritableSlice<JetExpression, Call> LOOP_RANGE_ITERATOR_CALL = Slices.createSimpleSlice();

    WritableSlice<JetExpression, ResolvedCall<FunctionDescriptor>> LOOP_RANGE_HAS_NEXT_RESOLVED_CALL = Slices.createSimpleSlice();
    WritableSlice<JetExpression, ResolvedCall<FunctionDescriptor>> LOOP_RANGE_NEXT_RESOLVED_CALL = Slices.createSimpleSlice();

    WritableSlice<PropertyAccessorDescriptor, ResolvedCall<FunctionDescriptor>> DELEGATED_PROPERTY_RESOLVED_CALL = Slices.createSimpleSlice();
    WritableSlice<PropertyAccessorDescriptor, Call> DELEGATED_PROPERTY_CALL = Slices.createSimpleSlice();

    WritableSlice<JetMultiDeclarationEntry, ResolvedCall<FunctionDescriptor>> COMPONENT_RESOLVED_CALL = Slices.createSimpleSlice();

    WritableSlice<JetExpression, ResolvedCall<FunctionDescriptor>> INDEXED_LVALUE_GET = Slices.createSimpleSlice();
    WritableSlice<JetExpression, ResolvedCall<FunctionDescriptor>> INDEXED_LVALUE_SET = Slices.createSimpleSlice();

    WritableSlice<JetExpression, JetType> AUTOCAST = Slices.createSimpleSlice();

    /**
     * A scope where type of expression has been resolved
     */
    WritableSlice<JetTypeReference, JetScope> TYPE_RESOLUTION_SCOPE = Slices.createSimpleSlice();
    WritableSlice<JetExpression, JetScope> RESOLUTION_SCOPE = Slices.createSimpleSlice();

    WritableSlice<ScriptDescriptor, JetScope> SCRIPT_SCOPE = Slices.createSimpleSlice();

    /**
     * Collected during analyze, used in IDE in auto-cast completion
     */
    WritableSlice<JetExpression, DataFlowInfo> NON_DEFAULT_EXPRESSION_DATA_FLOW = Slices.createSimpleSlice();

    WritableSlice<JetExpression, Boolean> VARIABLE_REASSIGNMENT = Slices.createSimpleSetSlice();
    WritableSlice<ValueParameterDescriptor, Boolean> AUTO_CREATED_IT = Slices.createSimpleSetSlice();
    WritableSlice<JetExpression, DeclarationDescriptor> VARIABLE_ASSIGNMENT = Slices.createSimpleSlice();

    /**
     * Has type of current expression has been already resolved
     */
    WritableSlice<JetExpression, Boolean> PROCESSED = Slices.createSimpleSetSlice();
    WritableSlice<JetElement, Boolean> STATEMENT = Slices.createRemovableSetSlice();

    WritableSlice<VariableDescriptor, CaptureKind> CAPTURED_IN_CLOSURE = new BasicWritableSlice<VariableDescriptor, CaptureKind>(DO_NOTHING);

    //    enum DeferredTypeKey {DEFERRED_TYPE_KEY}
    //    WritableSlice<DeferredTypeKey, Collection<DeferredType>> DEFERRED_TYPES = Slices.createSimpleSlice();

    WritableSlice<Box<DeferredType>, Boolean> DEFERRED_TYPE = Slices.createCollectiveSetSlice();

    WritableSlice<VariableDescriptor, ClassDescriptor> OBJECT_DECLARATION_CLASS = Slices.createSimpleSlice();

    WritableSlice<PropertyDescriptor, Boolean> BACKING_FIELD_REQUIRED = new Slices.SetSlice<PropertyDescriptor>(DO_NOTHING) {
        @Override
        public Boolean computeValue(
                SlicedMap map,
                PropertyDescriptor propertyDescriptor,
                Boolean backingFieldRequired,
                boolean valueNotFound
        ) {
            if (propertyDescriptor.getKind() != CallableMemberDescriptor.Kind.DECLARATION) {
                return false;
            }
            backingFieldRequired = valueNotFound ? false : backingFieldRequired;
            assert backingFieldRequired != null;
            // TODO: user BindingContextAccessors
            PsiElement declarationPsiElement = map.get(BindingContextUtils.DESCRIPTOR_TO_DECLARATION, propertyDescriptor);
            if (declarationPsiElement instanceof JetParameter) {
                JetParameter jetParameter = (JetParameter) declarationPsiElement;
                return jetParameter.getValOrVarNode() != null ||
                       backingFieldRequired; // this part is unused because we do not allow access to constructor parameters in member bodies
            }
            if (propertyDescriptor.getModality() == Modality.ABSTRACT) return false;
            PropertyGetterDescriptor getter = propertyDescriptor.getGetter();
            PropertySetterDescriptor setter = propertyDescriptor.getSetter();
            if (getter == null) {
                return true;
            }
            else if (propertyDescriptor.isVar() && setter == null) {
                return true;
            }
            else if (setter != null && !setter.hasBody() && setter.getModality() != Modality.ABSTRACT) {
                return true;
            }
            else if (!getter.hasBody() && getter.getModality() != Modality.ABSTRACT) {
                return true;
            }
            return backingFieldRequired;
        }
    };
    WritableSlice<PropertyDescriptor, Boolean> IS_INITIALIZED = Slices.createSimpleSetSlice();

    WritableSlice<JetFunctionLiteralExpression, Boolean> BLOCK = new Slices.SetSlice<JetFunctionLiteralExpression>(DO_NOTHING) {
        @Override
        public Boolean computeValue(SlicedMap map, JetFunctionLiteralExpression expression, Boolean isBlock, boolean valueNotFound) {
            isBlock = valueNotFound ? false : isBlock;
            assert isBlock != null;
            return isBlock && !expression.getFunctionLiteral().hasParameterSpecification();
        }
    };

    WritableSlice<PsiElement, NamespaceDescriptor> NAMESPACE = Slices.<PsiElement, NamespaceDescriptor>sliceBuilder()
            .setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION).build();
    WritableSlice<PsiElement, ClassDescriptor> CLASS =
            Slices.<PsiElement, ClassDescriptor>sliceBuilder().setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION)
                    .build();
    WritableSlice<PsiElement, ScriptDescriptor> SCRIPT =
            Slices.<PsiElement, ScriptDescriptor>sliceBuilder().setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION)
                    .build();
    WritableSlice<JetTypeParameter, TypeParameterDescriptor> TYPE_PARAMETER =
            Slices.<JetTypeParameter, TypeParameterDescriptor>sliceBuilder()
                    .setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION).build();
    /**
     * @see BindingContextUtils#recordFunctionDeclarationToDescriptor(BindingTrace, PsiElement, SimpleFunctionDescriptor)}
     */
    WritableSlice<PsiElement, SimpleFunctionDescriptor> FUNCTION = Slices.<PsiElement, SimpleFunctionDescriptor>sliceBuilder()
            .setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION).build();
    WritableSlice<PsiElement, ConstructorDescriptor> CONSTRUCTOR = Slices.<PsiElement, ConstructorDescriptor>sliceBuilder()
            .setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION).build();
    WritableSlice<PsiElement, VariableDescriptor> VARIABLE =
            Slices.<PsiElement, VariableDescriptor>sliceBuilder().setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION)
                    .build();
    WritableSlice<JetParameter, VariableDescriptor> VALUE_PARAMETER = Slices.<JetParameter, VariableDescriptor>sliceBuilder()
            .setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION).build();
    WritableSlice<JetPropertyAccessor, PropertyAccessorDescriptor> PROPERTY_ACCESSOR =
            Slices.<JetPropertyAccessor, PropertyAccessorDescriptor>sliceBuilder()
                    .setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION).build();

    // normalize value to getOriginal(value)
    WritableSlice<PsiElement, PropertyDescriptor> PRIMARY_CONSTRUCTOR_PARAMETER =
            Slices.<PsiElement, PropertyDescriptor>sliceBuilder().setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION)
                    .build();
    WritableSlice<JetObjectDeclarationName, PropertyDescriptor> OBJECT_DECLARATION =
            Slices.<JetObjectDeclarationName, PropertyDescriptor>sliceBuilder()
                    .setOpposite((WritableSlice) BindingContextUtils.DESCRIPTOR_TO_DECLARATION).build();

    WritableSlice[] DECLARATIONS_TO_DESCRIPTORS = new WritableSlice[] {
            NAMESPACE, CLASS, TYPE_PARAMETER, FUNCTION, CONSTRUCTOR, VARIABLE, VALUE_PARAMETER, PROPERTY_ACCESSOR,
            PRIMARY_CONSTRUCTOR_PARAMETER, OBJECT_DECLARATION
    };

    ReadOnlySlice<PsiElement, DeclarationDescriptor> DECLARATION_TO_DESCRIPTOR = Slices.<PsiElement, DeclarationDescriptor>sliceBuilder()
            .setFurtherLookupSlices(DECLARATIONS_TO_DESCRIPTORS).build();

    WritableSlice<PsiElement, FunctionDescriptor> CALLABLE_REFERENCE = Slices.createSimpleSlice();

    WritableSlice<JetReferenceExpression, PsiElement> LABEL_TARGET = Slices.<JetReferenceExpression, PsiElement>sliceBuilder().build();
    WritableSlice<JetReferenceExpression, Collection<? extends PsiElement>> AMBIGUOUS_LABEL_TARGET =
            Slices.<JetReferenceExpression, Collection<? extends PsiElement>>sliceBuilder().build();
    WritableSlice<ValueParameterDescriptor, PropertyDescriptor> VALUE_PARAMETER_AS_PROPERTY =
            Slices.<ValueParameterDescriptor, PropertyDescriptor>sliceBuilder().build();

    WritableSlice<ValueParameterDescriptor, FunctionDescriptor> DATA_CLASS_COMPONENT_FUNCTION =
            Slices.<ValueParameterDescriptor, FunctionDescriptor>sliceBuilder().build();
    WritableSlice<ClassDescriptor, FunctionDescriptor> DATA_CLASS_COPY_FUNCTION =
            Slices.<ClassDescriptor, FunctionDescriptor>sliceBuilder().build();

    WritableSlice<FqName, ClassDescriptor> FQNAME_TO_CLASS_DESCRIPTOR = new BasicWritableSlice<FqName, ClassDescriptor>(DO_NOTHING, true);
    WritableSlice<FqName, NamespaceDescriptor> FQNAME_TO_NAMESPACE_DESCRIPTOR =
            new BasicWritableSlice<FqName, NamespaceDescriptor>(DO_NOTHING);
    WritableSlice<JetFile, NamespaceDescriptor> FILE_TO_NAMESPACE = Slices.createSimpleSlice();
    WritableSlice<NamespaceDescriptor, Collection<JetFile>> NAMESPACE_TO_FILES = Slices.createSimpleSlice();

    /**
     * Each namespace found in src must be registered here.
     */
    WritableSlice<NamespaceDescriptor, Boolean> NAMESPACE_IS_SRC = Slices.createSimpleSlice();

    WritableSlice<ClassDescriptor, Boolean> INCOMPLETE_HIERARCHY = Slices.createCollectiveSetSlice();

    WritableSlice<DeclarationDescriptor, List<String>> LOAD_FROM_JAVA_SIGNATURE_ERRORS =
            new BasicWritableSlice<DeclarationDescriptor, List<String>>(Slices.ONLY_REWRITE_TO_EQUAL, true);

    WritableSlice<CallableDescriptor, Boolean> IS_DECLARED_IN_JAVA = Slices.createSimpleSlice();
    WritableSlice<SimpleFunctionDescriptor, ClassDescriptor> SAM_CONSTRUCTOR_TO_INTERFACE = Slices.createSimpleSlice();
    WritableSlice<SimpleFunctionDescriptor, SimpleFunctionDescriptor> SAM_ADAPTER_FUNCTION_TO_ORIGINAL = Slices.createSimpleSlice();
    WritableSlice<CallableMemberDescriptor, DeclarationDescriptor> SOURCE_DESCRIPTOR_FOR_SYNTHESIZED = Slices.createSimpleSlice();

    @SuppressWarnings("UnusedDeclaration")
    @Deprecated // This field is needed only for the side effects of its initializer
            Void _static_initializer = BasicWritableSlice.initSliceDebugNames(BindingContext.class);

    Collection<Diagnostic> getDiagnostics();

    @Nullable
    <K, V> V get(ReadOnlySlice<K, V> slice, K key);

    // slice.isCollective() must be true
    @NotNull
    <K, V> Collection<K> getKeys(WritableSlice<K, V> slice);

    /** This method should be used only for debug and testing */
    @TestOnly
    @NotNull
    <K, V> ImmutableMap<K, V> getSliceContents(@NotNull ReadOnlySlice<K, V> slice);
}