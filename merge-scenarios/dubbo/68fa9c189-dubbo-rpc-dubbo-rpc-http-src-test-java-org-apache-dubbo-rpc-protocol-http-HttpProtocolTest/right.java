package org.apache.dubbo.rpc.protocol.http;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.beanutil.JavaBeanDescriptor;
import org.apache.dubbo.common.beanutil.JavaBeanSerializeUtil;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.serialize.nativejava.NativeJavaSerialization;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.service.GenericService;
import junit.framework.Assert;
import org.junit.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import static org.junit.Assert.fail;

public class HttpProtocolTest {

    @Test
    public void testHttpProtocol() {
        HttpServiceImpl server = new HttpServiceImpl();
        Assert.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        URL url = URL.valueOf("http://127.0.0.1:5342/" + HttpService.class.getName() + "?release=2.7.0");
        Exporter<HttpService> exporter = protocol.export(proxyFactory.getInvoker(server, HttpService.class, url));
        Invoker<HttpService> invoker = protocol.refer(HttpService.class, url);
        HttpService client = proxyFactory.getProxy(invoker);
        String result = client.sayHello("haha");
        Assert.assertTrue(server.isCalled());
        Assert.assertEquals("Hello, haha", result);
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testGenericInvoke() {
        HttpServiceImpl server = new HttpServiceImpl();
        Assert.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        URL url = URL.valueOf("http://127.0.0.1:5342/" + HttpService.class.getName() + "?release=2.7.0");
        Exporter<HttpService> exporter = protocol.export(proxyFactory.getInvoker(server, HttpService.class, url));
        Invoker<GenericService> invoker = protocol.refer(GenericService.class, url);
        GenericService client = proxyFactory.getProxy(invoker, true);
        String result = (String) client.$invoke("sayHello", new String[] { "java.lang.String" }, new Object[] { "haha" });
        Assert.assertTrue(server.isCalled());
        Assert.assertEquals("Hello, haha", result);
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testGenericInvokeWithNativeJava() throws IOException, ClassNotFoundException {
        HttpServiceImpl server = new HttpServiceImpl();
        Assert.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        URL url = URL.valueOf("http://127.0.0.1:5342/" + HttpService.class.getName() + "?release=2.7.0&generic=nativejava");
        Exporter<HttpService> exporter = protocol.export(proxyFactory.getInvoker(server, HttpService.class, url));
        Invoker<GenericService> invoker = protocol.refer(GenericService.class, url);
        GenericService client = proxyFactory.getProxy(invoker);
        Serialization serialization = new NativeJavaSerialization();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutput objectOutput = serialization.serialize(url, byteArrayOutputStream);
        objectOutput.writeObject("haha");
        objectOutput.flushBuffer();
        Object result = client.$invoke("sayHello", new String[] { "java.lang.String" }, new Object[] { byteArrayOutputStream.toByteArray() });
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream((byte[]) result);
        ObjectInput objectInput = serialization.deserialize(url, byteArrayInputStream);
        Assert.assertTrue(server.isCalled());
        Assert.assertEquals("Hello, haha", objectInput.readObject());
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testGenericInvokeWithBean() {
        HttpServiceImpl server = new HttpServiceImpl();
        Assert.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        URL url = URL.valueOf("http://127.0.0.1:5342/" + HttpService.class.getName() + "?release=2.7.0&generic=bean");
        Exporter<HttpService> exporter = protocol.export(proxyFactory.getInvoker(server, HttpService.class, url));
        Invoker<GenericService> invoker = protocol.refer(GenericService.class, url);
        GenericService client = proxyFactory.getProxy(invoker);
        JavaBeanDescriptor javaBeanDescriptor = JavaBeanSerializeUtil.serialize("haha");
        Object result = client.$invoke("sayHello", new String[] { "java.lang.String" }, new Object[] { javaBeanDescriptor });
        Assert.assertTrue(server.isCalled());
        Assert.assertEquals("Hello, haha", JavaBeanSerializeUtil.deserialize((JavaBeanDescriptor) result));
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testOverload() {
        HttpServiceImpl server = new HttpServiceImpl();
        Assert.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        URL url = URL.valueOf("http://127.0.0.1:5342/" + HttpService.class.getName() + "?release=2.7.0&hessian.overload.method=true&hessian2.request=false");
        Exporter<HttpService> exporter = protocol.export(proxyFactory.getInvoker(server, HttpService.class, url));
        Invoker<HttpService> invoker = protocol.refer(HttpService.class, url);
        HttpService client = proxyFactory.getProxy(invoker);
        String result = client.sayHello("haha");
        Assert.assertEquals("Hello, haha", result);
        result = client.sayHello("haha", 1);
        Assert.assertEquals("Hello, haha. ", result);
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testSimpleClient() {
        HttpServiceImpl server = new HttpServiceImpl();
        Assert.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        URL url = URL.valueOf("http://127.0.0.1:5342/" + HttpService.class.getName() + "?release=2.7.0&client=simple");
        Exporter<HttpService> exporter = protocol.export(proxyFactory.getInvoker(server, HttpService.class, url));
        Invoker<HttpService> invoker = protocol.refer(HttpService.class, url);
        HttpService client = proxyFactory.getProxy(invoker);
        String result = client.sayHello("haha");
        Assert.assertTrue(server.isCalled());
        Assert.assertEquals("Hello, haha", result);
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testTimeOut() {
        HttpServiceImpl server = new HttpServiceImpl();
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        URL url = URL.valueOf("http://127.0.0.1:5342/" + HttpService.class.getName() + "?release=2.7.0&timeout=10");
        Exporter<HttpService> exporter = protocol.export(proxyFactory.getInvoker(server, HttpService.class, url));
        Invoker<HttpService> invoker = protocol.refer(HttpService.class, url);
        HttpService client = proxyFactory.getProxy(invoker);
        try {
            client.timeOut(6000);
            fail();
        } catch (RpcException expected) {
            Assert.assertEquals(true, expected.isTimeout());
        } finally {
            invoker.destroy();
            exporter.unexport();
        }
    }

    @Test
    public void testCustomException() {
        HttpServiceImpl server = new HttpServiceImpl();
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        URL url = URL.valueOf("http://127.0.0.1:5342/" + HttpService.class.getName() + "?release=2.7.0");
        Exporter<HttpService> exporter = protocol.export(proxyFactory.getInvoker(server, HttpService.class, url));
        Invoker<HttpService> invoker = protocol.refer(HttpService.class, url);
        HttpService client = proxyFactory.getProxy(invoker);
        try {
            client.customException();
            fail();
        } catch (HttpServiceImpl.MyException expected) {
        }
        invoker.destroy();
        exporter.unexport();
    }
}