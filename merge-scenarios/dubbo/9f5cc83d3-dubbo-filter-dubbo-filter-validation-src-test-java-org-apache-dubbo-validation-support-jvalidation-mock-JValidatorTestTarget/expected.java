package org.apache.dubbo.validation.support.jvalidation.mock;

import org.apache.dubbo.validation.MethodValidated;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public interface JValidatorTestTarget {

    @MethodValidated
    void someMethod1(String anything);

    @MethodValidated(Test2.class)
    void someMethod2(@NotNull ValidationParameter validationParameter);

    void someMethod3(ValidationParameter[] parameters);

    void someMethod4(List<String> strings);

    void someMethod5(Map<String, String> map);

    public void someMethod3(ValidationParameter[] parameters);

    public void someMethod4(List<String> strings);

    public void someMethod5(Map<String, String> map);

    @interface Test2 {
    }
}