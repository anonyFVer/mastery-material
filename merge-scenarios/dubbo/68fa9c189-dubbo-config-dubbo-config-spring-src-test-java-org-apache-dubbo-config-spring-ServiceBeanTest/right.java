package org.apache.dubbo.config.spring;

import org.apache.dubbo.config.annotation.Service;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.Mockito.mock;

public class ServiceBeanTest {

    @Test
    public void testGetService() {
        TestService service = mock(TestService.class);
        ServiceBean serviceBean = new ServiceBean(service);
        Service beanService = serviceBean.getService();
        Assert.assertThat(beanService, not(nullValue()));
    }

    abstract class TestService implements Service {
    }
}