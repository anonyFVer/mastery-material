package org.apache.dubbo.config.spring.status;

import org.apache.dubbo.common.status.Status;
import org.apache.dubbo.config.spring.ServiceBean;
import org.apache.dubbo.config.spring.extension.SpringExtensionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.context.Lifecycle;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.initMocks;

public class SpringStatusCheckerTest {

    private SpringStatusChecker springStatusChecker;

    @Mock
    private ApplicationContext applicationContext;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        this.springStatusChecker = new SpringStatusChecker();
        new ServiceBean<Object>().setApplicationContext(applicationContext);
    }

    @After
    public void tearDown() throws Exception {
        SpringExtensionFactory.clearContexts();
        Mockito.reset(applicationContext);
    }

    @Test
    public void testWithoutApplicationContext() {
        Status status = springStatusChecker.check();
        assertThat(status.getLevel(), is(Status.Level.UNKNOWN));
    }

    @Test
    public void testWithLifeCycleRunning() {
        SpringExtensionFactory.clearContexts();
        ApplicationLifeCycle applicationLifeCycle = mock(ApplicationLifeCycle.class);
        new ServiceBean<Object>().setApplicationContext(applicationLifeCycle);
        given(applicationLifeCycle.getConfigLocations()).willReturn(new String[] { "test1", "test2" });
        given(applicationLifeCycle.isRunning()).willReturn(true);
        Status status = springStatusChecker.check();
        assertThat(status.getLevel(), is(Status.Level.OK));
        assertThat(status.getMessage(), is("test1,test2"));
    }

    @Test
    public void testWithoutLifeCycleRunning() {
        SpringExtensionFactory.clearContexts();
        ApplicationLifeCycle applicationLifeCycle = mock(ApplicationLifeCycle.class);
        new ServiceBean<Object>().setApplicationContext(applicationLifeCycle);
        given(applicationLifeCycle.isRunning()).willReturn(false);
        Status status = springStatusChecker.check();
        assertThat(status.getLevel(), is(Status.Level.ERROR));
    }

    interface ApplicationLifeCycle extends Lifecycle, ApplicationContext {

        String[] getConfigLocations();
    }
}