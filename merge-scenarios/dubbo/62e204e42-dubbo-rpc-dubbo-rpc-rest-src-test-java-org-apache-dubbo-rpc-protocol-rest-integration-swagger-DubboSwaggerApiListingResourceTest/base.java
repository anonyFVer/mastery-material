package org.apache.dubbo.rpc.protocol.rest.integration.swagger;

import io.swagger.models.Swagger;
import org.jboss.resteasy.spi.ResteasyUriInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import static org.mockito.Mockito.*;

public class DubboSwaggerApiListingResourceTest {

    private Application app;

    private ServletConfig sc;

    @Test
    public void test() throws Exception {
        DubboSwaggerApiListingResource resource = new DubboSwaggerApiListingResource();
        app = mock(Application.class);
        sc = mock(ServletConfig.class);
        Set<Class<?>> sets = new HashSet<Class<?>>();
        sets.add(SwaggerService.class);
        when(sc.getServletContext()).thenReturn(mock(ServletContext.class));
        when(app.getClasses()).thenReturn(sets);
        Response response = resource.getListingJson(app, sc, null, new ResteasyUriInfo(new URI("http://rest.test")));
        Assertions.assertNotNull(response);
        Swagger swagger = (Swagger) response.getEntity();
        Assertions.assertEquals("SwaggerService", swagger.getTags().get(0).getName());
        Assertions.assertEquals("/demoService/hello", swagger.getPaths().keySet().toArray()[0].toString());
    }
}