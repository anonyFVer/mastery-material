package org.apache.dubbo.config;

import org.apache.dubbo.rpc.model.ConsumerMethodModel;
import org.apache.dubbo.service.Person;
import com.alibaba.dubbo.config.ArgumentConfig;
import com.alibaba.dubbo.config.MethodConfig;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import static org.apache.dubbo.config.Constants.ON_INVOKE_INSTANCE_KEY;
import static org.apache.dubbo.config.Constants.ON_INVOKE_METHOD_KEY;
import static org.apache.dubbo.config.Constants.ON_RETURN_INSTANCE_KEY;
import static org.apache.dubbo.config.Constants.ON_RETURN_METHOD_KEY;
import static org.apache.dubbo.config.Constants.ON_THROW_INSTANCE_KEY;
import static org.apache.dubbo.config.Constants.ON_THROW_METHOD_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MethodConfigTest {

    @Test
    public void testName() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setName("hello");
        assertThat(method.getName(), equalTo("hello"));
        Map<String, String> parameters = new HashMap<String, String>();
        MethodConfig.appendParameters(parameters, method);
        assertThat(parameters, not(hasKey("name")));
    }

    @Test
    public void testStat() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setStat(10);
        assertThat(method.getStat(), equalTo(10));
    }

    @Test
    public void testRetry() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setRetry(true);
        assertThat(method.isRetry(), is(true));
    }

    @Test
    public void testReliable() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setReliable(true);
        assertThat(method.isReliable(), is(true));
    }

    @Test
    public void testExecutes() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setExecutes(10);
        assertThat(method.getExecutes(), equalTo(10));
    }

    @Test
    public void testDeprecated() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setDeprecated(true);
        assertThat(method.getDeprecated(), is(true));
    }

    @Test
    public void testArguments() throws Exception {
        MethodConfig method = new MethodConfig();
        ArgumentConfig argument = new ArgumentConfig();
        method.setArguments(Collections.singletonList(argument));
        assertThat(method.getArguments(), contains(argument));
        assertThat(method.getArguments(), Matchers.<org.apache.dubbo.config.ArgumentConfig>hasSize(1));
    }

    @Test
    public void testSticky() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setSticky(true);
        assertThat(method.getSticky(), is(true));
    }

    @Test
    public void testConvertMethodConfig2AsyncInfo() throws Exception {
        org.apache.dubbo.config.MethodConfig methodConfig = new org.apache.dubbo.config.MethodConfig();
        methodConfig.setOninvokeMethod("setName");
        methodConfig.setOninvoke(new Person());
        ConsumerMethodModel.AsyncMethodInfo methodInfo = org.apache.dubbo.config.MethodConfig.convertMethodConfig2AsyncInfo(methodConfig);
        assertEquals(methodInfo.getOninvokeMethod(), Person.class.getMethod("setName", String.class));
    }

    @Test
    public void testOnreturn() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setOnreturn("on-return-object");
        assertThat(method.getOnreturn(), equalTo((Object) "on-return-object"));
        Map<String, Object> attribute = new HashMap<String, Object>();
        MethodConfig.appendAttributes(attribute, method);
        assertThat(attribute, hasEntry((Object) ON_RETURN_INSTANCE_KEY, (Object) "on-return-object"));
        Map<String, String> parameters = new HashMap<String, String>();
        MethodConfig.appendParameters(parameters, method);
        assertThat(parameters.size(), is(0));
    }

    @Test
    public void testOnreturnMethod() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setOnreturnMethod("on-return-method");
        assertThat(method.getOnreturnMethod(), equalTo("on-return-method"));
        Map<String, Object> attribute = new HashMap<String, Object>();
        MethodConfig.appendAttributes(attribute, method);
        assertThat(attribute, hasEntry((Object) ON_RETURN_METHOD_KEY, (Object) "on-return-method"));
        Map<String, String> parameters = new HashMap<String, String>();
        MethodConfig.appendParameters(parameters, method);
        assertThat(parameters.size(), is(0));
    }

    @Test
    public void testOnthrow() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setOnthrow("on-throw-object");
        assertThat(method.getOnthrow(), equalTo((Object) "on-throw-object"));
        Map<String, Object> attribute = new HashMap<String, Object>();
        MethodConfig.appendAttributes(attribute, method);
        assertThat(attribute, hasEntry((Object) ON_THROW_INSTANCE_KEY, (Object) "on-throw-object"));
        Map<String, String> parameters = new HashMap<String, String>();
        MethodConfig.appendParameters(parameters, method);
        assertThat(parameters.size(), is(0));
    }

    @Test
    public void testOnthrowMethod() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setOnthrowMethod("on-throw-method");
        assertThat(method.getOnthrowMethod(), equalTo("on-throw-method"));
        Map<String, Object> attribute = new HashMap<String, Object>();
        MethodConfig.appendAttributes(attribute, method);
        assertThat(attribute, hasEntry((Object) ON_THROW_METHOD_KEY, (Object) "on-throw-method"));
        Map<String, String> parameters = new HashMap<String, String>();
        MethodConfig.appendParameters(parameters, method);
        assertThat(parameters.size(), is(0));
    }

    @Test
    public void testOninvoke() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setOninvoke("on-invoke-object");
        assertThat(method.getOninvoke(), equalTo((Object) "on-invoke-object"));
        Map<String, Object> attribute = new HashMap<String, Object>();
        MethodConfig.appendAttributes(attribute, method);
        assertThat(attribute, hasEntry((Object) ON_INVOKE_INSTANCE_KEY, (Object) "on-invoke-object"));
        Map<String, String> parameters = new HashMap<String, String>();
        MethodConfig.appendParameters(parameters, method);
        assertThat(parameters.size(), is(0));
    }

    @Test
    public void testOninvokeMethod() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setOninvokeMethod("on-invoke-method");
        assertThat(method.getOninvokeMethod(), equalTo("on-invoke-method"));
        Map<String, Object> attribute = new HashMap<String, Object>();
        MethodConfig.appendAttributes(attribute, method);
        assertThat(attribute, hasEntry((Object) ON_INVOKE_METHOD_KEY, (Object) "on-invoke-method"));
        Map<String, String> parameters = new HashMap<String, String>();
        MethodConfig.appendParameters(parameters, method);
        assertThat(parameters.size(), is(0));
    }

    @Test
    public void testReturn() throws Exception {
        MethodConfig method = new MethodConfig();
        method.setReturn(true);
        assertThat(method.isReturn(), is(true));
    }
}