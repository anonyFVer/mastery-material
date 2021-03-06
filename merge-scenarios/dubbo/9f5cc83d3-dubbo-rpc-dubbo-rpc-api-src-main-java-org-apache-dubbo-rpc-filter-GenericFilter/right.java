package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.beanutil.JavaBeanAccessor;
import org.apache.dubbo.common.beanutil.JavaBeanDescriptor;
import org.apache.dubbo.common.beanutil.JavaBeanSerializeUtil;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.io.UnsafeByteArrayInputStream;
import org.apache.dubbo.common.io.UnsafeByteArrayOutputStream;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.PojoUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ListenableFilter;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.service.GenericException;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;
import java.io.IOException;
import java.lang.reflect.Method;

@Activate(group = Constants.PROVIDER, order = -20000)
public class GenericFilter extends ListenableFilter {

    public GenericFilter() {
        super.listener = new GenericListener();
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        if ((inv.getMethodName().equals(Constants.$INVOKE) || inv.getMethodName().equals(Constants.$INVOKE_ASYNC)) && inv.getArguments() != null && inv.getArguments().length == 3 && !GenericService.class.isAssignableFrom(invoker.getInterface())) {
            String name = ((String) inv.getArguments()[0]).trim();
            String[] types = (String[]) inv.getArguments()[1];
            Object[] args = (Object[]) inv.getArguments()[2];
            try {
                Method method = ReflectUtils.findMethodByMethodSignature(invoker.getInterface(), name, types);
                Class<?>[] params = method.getParameterTypes();
                if (args == null) {
                    args = new Object[params.length];
                }
                String generic = (String) inv.getAttachment(Constants.GENERIC_KEY);
                if (StringUtils.isBlank(generic)) {
                    generic = (String) RpcContext.getContext().getAttachment(Constants.GENERIC_KEY);
                }
                if (StringUtils.isEmpty(generic) || ProtocolUtils.isDefaultGenericSerialization(generic)) {
                    args = PojoUtils.realize(args, params, method.getGenericParameterTypes());
                } else if (ProtocolUtils.isJavaGenericSerialization(generic)) {
                    for (int i = 0; i < args.length; i++) {
                        if (byte[].class == args[i].getClass()) {
                            try (UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream((byte[]) args[i])) {
                                args[i] = ExtensionLoader.getExtensionLoader(Serialization.class).getExtension(Constants.GENERIC_SERIALIZATION_NATIVE_JAVA).deserialize(null, is).readObject();
                            } catch (Exception e) {
                                throw new RpcException("Deserialize argument [" + (i + 1) + "] failed.", e);
                            }
                        } else {
                            throw new RpcException("Generic serialization [" + Constants.GENERIC_SERIALIZATION_NATIVE_JAVA + "] only support message type " + byte[].class + " and your message type is " + args[i].getClass());
                        }
                    }
                } else if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                    for (int i = 0; i < args.length; i++) {
                        if (args[i] instanceof JavaBeanDescriptor) {
                            args[i] = JavaBeanSerializeUtil.deserialize((JavaBeanDescriptor) args[i]);
                        } else {
                            throw new RpcException("Generic serialization [" + Constants.GENERIC_SERIALIZATION_BEAN + "] only support message type " + JavaBeanDescriptor.class.getName() + " and your message type is " + args[i].getClass().getName());
                        }
                    }
                }
                return invoker.invoke(new RpcInvocation(method, args, inv.getAttachments(), inv.getAttributes()));
            } catch (NoSuchMethodException e) {
                throw new RpcException(e.getMessage(), e);
            } catch (ClassNotFoundException e) {
                throw new RpcException(e.getMessage(), e);
            }
        }
        return invoker.invoke(inv);
    }

    static class GenericListener implements Listener {

        @Override
        public void onResponse(Result appResponse, Invoker<?> invoker, Invocation inv) {
            if ((inv.getMethodName().equals(Constants.$INVOKE) || inv.getMethodName().equals(Constants.$INVOKE_ASYNC)) && inv.getArguments() != null && inv.getArguments().length == 3 && !GenericService.class.isAssignableFrom(invoker.getInterface())) {
                String generic = (String) inv.getAttachment(Constants.GENERIC_KEY);
                if (StringUtils.isBlank(generic)) {
                    generic = (String) RpcContext.getContext().getAttachment(Constants.GENERIC_KEY);
                }
                if (appResponse.hasException() && !(appResponse.getException() instanceof GenericException)) {
                    appResponse.setException(new GenericException(appResponse.getException()));
                }
                if (ProtocolUtils.isJavaGenericSerialization(generic)) {
                    try {
                        UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream(512);
                        ExtensionLoader.getExtensionLoader(Serialization.class).getExtension(Constants.GENERIC_SERIALIZATION_NATIVE_JAVA).serialize(null, os).writeObject(appResponse.getValue());
                        appResponse.setValue(os.toByteArray());
                    } catch (IOException e) {
                        throw new RpcException("Serialize result failed.", e);
                    }
                } else if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                    appResponse.setValue(JavaBeanSerializeUtil.serialize(appResponse.getValue(), JavaBeanAccessor.METHOD));
                } else {
                    appResponse.setValue(PojoUtils.generalize(appResponse.getValue()));
                }
            }
        }

        @Override
        public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        }
    }
}