package org.apache.dubbo.qos.command.impl;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.qos.command.BaseCommand;
import org.apache.dubbo.qos.command.CommandContext;
import org.apache.dubbo.qos.command.annotation.Cmd;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.model.invoker.ProviderInvokerWrapper;
import java.util.Collection;

@Cmd(name = "offline", summary = "offline dubbo", example = { "offline dubbo", "offline xx.xx.xxx.service" })
public class Offline implements BaseCommand {

    private Logger logger = LoggerFactory.getLogger(Offline.class);

    private RegistryFactory registryFactory = ExtensionLoader.getExtensionLoader(RegistryFactory.class).getAdaptiveExtension();

    @Override
    public String execute(CommandContext commandContext, String[] args) {
        logger.info("receive offline command");
        String servicePattern = ".*";
        if (args != null && args.length > 0) {
            servicePattern = args[0];
        }
        boolean hasService = false;
        Collection<ProviderModel> providerModelList = ApplicationModel.allProviderModels();
        for (ProviderModel providerModel : providerModelList) {
            if (providerModel.getServiceKey().matches(servicePattern)) {
                hasService = true;
                Collection<ProviderInvokerWrapper> providerInvokerWrapperSet = ApplicationModel.getProviderInvokers(providerModel.getServiceKey());
                for (ProviderInvokerWrapper providerInvokerWrapper : providerInvokerWrapperSet) {
                    if (!providerInvokerWrapper.isReg()) {
                        continue;
                    }
                    Registry registry = registryFactory.getRegistry(providerInvokerWrapper.getRegistryUrl());
                    registry.unregister(providerInvokerWrapper.getProviderUrl());
                    providerInvokerWrapper.setReg(false);
                }
            }
        }
        if (hasService) {
            return "OK";
        } else {
            return "service not found";
        }
    }
}