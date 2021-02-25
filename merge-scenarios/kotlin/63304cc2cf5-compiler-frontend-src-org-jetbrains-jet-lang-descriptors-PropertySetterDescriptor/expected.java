package org.jetbrains.jet.lang.descriptors;

import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.jet.lang.descriptors.annotations.AnnotationDescriptor;
import org.jetbrains.jet.lang.types.JetStandardClasses;
import org.jetbrains.jet.lang.types.JetType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author abreslav
 */
public class PropertySetterDescriptor extends PropertyAccessorDescriptor {

    private MutableValueParameterDescriptor parameter;

    public PropertySetterDescriptor(@NotNull Modality modality, @NotNull Visibility visibility, @NotNull PropertyDescriptor correspondingProperty, @NotNull List<AnnotationDescriptor> annotations, boolean hasBody, boolean isDefault) {
        super(modality, visibility, correspondingProperty, annotations, "set-" + correspondingProperty.getName(), hasBody, isDefault);
    }

    public void initialize(@NotNull MutableValueParameterDescriptor parameter) {
        assert this.parameter == null;
        this.parameter = parameter;
    }

    public void setParameterType(@NotNull JetType type) {
        parameter.setType(type);
    }

    @NotNull
    @Override
    public Set<? extends PropertyAccessorDescriptor> getOverriddenDescriptors() {
        return super.getOverriddenDescriptors(false);
    }

    @NotNull
    @Override
    public List<ValueParameterDescriptor> getValueParameters() {
        return Collections.<ValueParameterDescriptor>singletonList(parameter);
    }

    @NotNull
    @Override
    public JetType getReturnType() {
        return JetStandardClasses.getUnitType();
    }

    @Override
    public JetType getReturnTypeSafe() {
        return getReturnType();
    }

    @Override
    public <R, D> R accept(DeclarationDescriptorVisitor<R, D> visitor, D data) {
        return visitor.visitPropertySetterDescriptor(this, data);
    }
}