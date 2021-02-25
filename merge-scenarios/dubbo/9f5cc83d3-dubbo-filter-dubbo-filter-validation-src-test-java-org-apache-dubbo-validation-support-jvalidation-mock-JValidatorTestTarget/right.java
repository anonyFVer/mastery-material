package org.apache.dubbo.validation.support.jvalidation.mock;

import org.apache.dubbo.validation.MethodValidated;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public interface JValidatorTestTarget {

    @MethodValidated
    public void someMethod1(String anything);

    @MethodValidated(Test2.class)
    public void someMethod2(@NotNull ValidationParameter validationParameter);

    public void someMethod3(ValidationParameter[] parameters);

    public void someMethod4(List<String> strings);

    public void someMethod5(Map<String, String> map);

    @interface Test2 {
    }
}