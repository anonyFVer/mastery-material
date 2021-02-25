package org.apache.dubbo.validation.support.jvalidation.mock;

import org.apache.dubbo.validation.MethodValidated;
import javax.validation.constraints.NotNull;

public interface JValidatorTestTarget {

    @MethodValidated
    public void someMethod1(String anything);

    @MethodValidated(Test2.class)
    public void someMethod2(@NotNull ValidationParameter validationParameter);

    @interface Test2 {
    }
}