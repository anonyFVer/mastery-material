package org.apache.dubbo.rpc.validation;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.service.GenericException;
import org.apache.dubbo.rpc.service.GenericService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ValidationTest {

    private ApplicationConfig application = new ApplicationConfig("validation-test");

    private RegistryConfig registryNA = new RegistryConfig("N/A");

    private ProtocolConfig protocolDubo29582 = new ProtocolConfig("dubbo", 29582);

    @Before
    public void setUp() {
        ConfigManager.getInstance().clear();
    }

    @After
    public void tearDown() {
        ConfigManager.getInstance().clear();
    }

    @Test
    public void testValidation() {
        ServiceConfig<ValidationService> service = new ServiceConfig<ValidationService>();
        service.setApplication(application);
        service.setRegistry(registryNA);
        service.setProtocol(protocolDubo29582);
        service.setInterface(ValidationService.class.getName());
        service.setRef(new ValidationServiceImpl());
        service.setValidation(String.valueOf(true));
        service.export();
        try {
            ReferenceConfig<ValidationService> reference = new ReferenceConfig<ValidationService>();
            reference.setApplication(application);
            reference.setInterface(ValidationService.class);
            reference.setUrl("dubbo://127.0.0.1:29582?scope=remote&validation=true");
            ValidationService validationService = reference.get();
            try {
                ValidationParameter parameter = new ValidationParameter();
                parameter.setName("liangfei");
                parameter.setEmail("liangfei@liang.fei");
                parameter.setAge(50);
                parameter.setLoginDate(new Date(System.currentTimeMillis() - 1000000));
                parameter.setExpiryDate(new Date(System.currentTimeMillis() + 1000000));
                validationService.save(parameter);
                try {
                    parameter = new ValidationParameter();
                    parameter.setName("l");
                    parameter.setEmail("liangfei@liang.fei");
                    parameter.setAge(50);
                    parameter.setLoginDate(new Date(System.currentTimeMillis() - 1000000));
                    parameter.setExpiryDate(new Date(System.currentTimeMillis() + 1000000));
                    validationService.save(parameter);
                    Assertions.fail();
                } catch (ConstraintViolationException ve) {
                    Set<ConstraintViolation<?>> violations = ve.getConstraintViolations();
                    Assertions.assertNotNull(violations);
                }
                try {
                    parameter = new ValidationParameter();
                    parameter.setName("liangfei");
                    parameter.setAge(50);
                    parameter.setLoginDate(new Date(System.currentTimeMillis() - 1000000));
                    parameter.setExpiryDate(new Date(System.currentTimeMillis() + 1000000));
                    validationService.save(parameter);
                    Assertions.fail();
                } catch (ConstraintViolationException ve) {
                    Set<ConstraintViolation<?>> violations = ve.getConstraintViolations();
                    Assertions.assertNotNull(violations);
                }
                try {
                    parameter = new ValidationParameter();
                    parameter.setName("liangfei");
                    parameter.setAge(50);
                    parameter.setLoginDate(new Date(System.currentTimeMillis() - 1000000));
                    parameter.setExpiryDate(new Date(System.currentTimeMillis() + 1000000));
                    validationService.relatedQuery(parameter);
                    Assertions.fail();
                } catch (ConstraintViolationException ve) {
                    Set<ConstraintViolation<?>> violations = ve.getConstraintViolations();
                    Assertions.assertEquals(violations.size(), 2);
                }
                try {
                    parameter = new ValidationParameter();
                    validationService.save(parameter);
                    Assertions.fail();
                } catch (ConstraintViolationException ve) {
                    Set<ConstraintViolation<?>> violations = ve.getConstraintViolations();
                    Assertions.assertTrue(violations.size() == 3);
                    Assertions.assertNotNull(violations);
                }
                validationService.delete(2, "abc");
                try {
                    validationService.delete(2, "a");
                    Assertions.fail();
                } catch (ConstraintViolationException ve) {
                    Set<ConstraintViolation<?>> violations = ve.getConstraintViolations();
                    Assertions.assertNotNull(violations);
                    Assertions.assertEquals(1, violations.size());
                }
                try {
                    validationService.delete(0, "abc");
                    Assertions.fail();
                } catch (ConstraintViolationException ve) {
                    Set<ConstraintViolation<?>> violations = ve.getConstraintViolations();
                    Assertions.assertNotNull(violations);
                    Assertions.assertEquals(1, violations.size());
                }
                try {
                    validationService.delete(2, null);
                    Assertions.fail();
                } catch (ConstraintViolationException ve) {
                    Set<ConstraintViolation<?>> violations = ve.getConstraintViolations();
                    Assertions.assertNotNull(violations);
                    Assertions.assertEquals(1, violations.size());
                }
                try {
                    validationService.delete(0, null);
                    Assertions.fail();
                } catch (ConstraintViolationException ve) {
                    Set<ConstraintViolation<?>> violations = ve.getConstraintViolations();
                    Assertions.assertNotNull(violations);
                    Assertions.assertEquals(2, violations.size());
                }
            } finally {
                reference.destroy();
            }
        } finally {
            service.unexport();
        }
    }

    @Test
    public void testProviderValidation() {
        ServiceConfig<ValidationService> service = new ServiceConfig<ValidationService>();
        service.setApplication(application);
        service.setRegistry(registryNA);
        service.setProtocol(protocolDubo29582);
        service.setInterface(ValidationService.class.getName());
        service.setRef(new ValidationServiceImpl());
        service.setValidation(String.valueOf(true));
        service.export();
        try {
            ReferenceConfig<ValidationService> reference = new ReferenceConfig<ValidationService>();
            reference.setApplication(application);
            reference.setInterface(ValidationService.class);
            reference.setUrl("dubbo://127.0.0.1:29582");
            ValidationService validationService = reference.get();
            try {
                ValidationParameter parameter = new ValidationParameter();
                parameter.setName("liangfei");
                parameter.setEmail("liangfei@liang.fei");
                parameter.setAge(50);
                parameter.setLoginDate(new Date(System.currentTimeMillis() - 1000000));
                parameter.setExpiryDate(new Date(System.currentTimeMillis() + 1000000));
                validationService.save(parameter);
                try {
                    parameter = new ValidationParameter();
                    validationService.save(parameter);
                    Assertions.fail();
                } catch (RpcException e) {
                    Assertions.assertTrue(e.getMessage().contains("ConstraintViolation"));
                }
                validationService.delete(2, "abc");
                try {
                    validationService.delete(0, "abc");
                    Assertions.fail();
                } catch (RpcException e) {
                    Assertions.assertTrue(e.getMessage().contains("ConstraintViolation"));
                }
                try {
                    validationService.delete(2, null);
                    Assertions.fail();
                } catch (RpcException e) {
                    Assertions.assertTrue(e.getMessage().contains("ConstraintViolation"));
                }
                try {
                    validationService.delete(0, null);
                    Assertions.fail();
                } catch (RpcException e) {
                    Assertions.assertTrue(e.getMessage().contains("ConstraintViolation"));
                }
            } finally {
                reference.destroy();
            }
        } finally {
            service.unexport();
        }
    }

    @Test
    public void testGenericValidation() {
        ServiceConfig<ValidationService> service = new ServiceConfig<ValidationService>();
        service.setApplication(application);
        service.setRegistry(registryNA);
        service.setProtocol(protocolDubo29582);
        service.setInterface(ValidationService.class.getName());
        service.setRef(new ValidationServiceImpl());
        service.setValidation(String.valueOf(true));
        service.export();
        try {
            ReferenceConfig<GenericService> reference = new ReferenceConfig<GenericService>();
            reference.setApplication(application);
            reference.setInterface(ValidationService.class.getName());
            reference.setUrl("dubbo://127.0.0.1:29582?scope=remote&validation=true&timeout=9000000");
            reference.setGeneric(true);
            GenericService validationService = reference.get();
            try {
                Map<String, Object> parameter = new HashMap<String, Object>();
                parameter.put("name", "liangfei");
                parameter.put("Email", "liangfei@liang.fei");
                parameter.put("Age", 50);
                parameter.put("LoginDate", new Date(System.currentTimeMillis() - 1000000));
                parameter.put("ExpiryDate", new Date(System.currentTimeMillis() + 1000000));
                validationService.$invoke("save", new String[] { ValidationParameter.class.getName() }, new Object[] { parameter });
                try {
                    parameter = new HashMap<String, Object>();
                    validationService.$invoke("save", new String[] { ValidationParameter.class.getName() }, new Object[] { parameter });
                    Assertions.fail();
                } catch (GenericException e) {
                    Assertions.assertTrue(e.getMessage().contains("Failed to validate service"));
                }
                validationService.$invoke("delete", new String[] { long.class.getName(), String.class.getName() }, new Object[] { 2, "abc" });
                try {
                    validationService.$invoke("delete", new String[] { long.class.getName(), String.class.getName() }, new Object[] { 0, "abc" });
                    Assertions.fail();
                } catch (GenericException e) {
                    Assertions.assertTrue(e.getMessage().contains("Failed to validate service"));
                }
                try {
                    validationService.$invoke("delete", new String[] { long.class.getName(), String.class.getName() }, new Object[] { 2, null });
                    Assertions.fail();
                } catch (GenericException e) {
                    Assertions.assertTrue(e.getMessage().contains("Failed to validate service"));
                }
                try {
                    validationService.$invoke("delete", new String[] { long.class.getName(), String.class.getName() }, new Object[] { 0, null });
                    Assertions.fail();
                } catch (GenericException e) {
                    Assertions.assertTrue(e.getMessage().contains("Failed to validate service"));
                }
            } catch (GenericException e) {
                Assertions.assertTrue(e.getMessage().contains("Failed to validate service"));
            } finally {
                reference.destroy();
            }
        } finally {
            service.unexport();
        }
    }
}