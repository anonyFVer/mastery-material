package org.apache.dubbo.qos.command.impl;

import org.apache.dubbo.qos.command.BaseCommand;
import org.apache.dubbo.qos.command.CommandContext;
import org.apache.dubbo.qos.command.annotation.Cmd;
import org.apache.dubbo.qos.textui.TTable;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ProviderModel;
import java.util.Collection;
import static org.apache.dubbo.registry.support.ProviderConsumerRegTable.getConsumerAddressNum;
import static org.apache.dubbo.registry.support.ProviderConsumerRegTable.isRegistered;

@Cmd(name = "ls", summary = "ls service", example = { "ls" })
public class Ls implements BaseCommand {

    @Override
    public String execute(CommandContext commandContext, String[] args) {
        StringBuilder result = new StringBuilder();
        result.append(listProvider());
        result.append(listConsumer());
        return result.toString();
    }

    public String listProvider() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("As Provider side:" + System.lineSeparator());
        Collection<ProviderModel> providerModelList = ApplicationModel.allProviderModels();
        TTable tTable = new TTable(new TTable.ColumnDefine[] { new TTable.ColumnDefine(TTable.Align.MIDDLE), new TTable.ColumnDefine(TTable.Align.MIDDLE) });
        tTable.addRow("Provider Service Name", "PUB");
        for (ProviderModel providerModel : providerModelList) {
            tTable.addRow(providerModel.getServiceName(), isRegistered(providerModel.getServiceName()) ? "Y" : "N");
        }
        stringBuilder.append(tTable.rendering());
        return stringBuilder.toString();
    }

    public String listConsumer() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("As Consumer side:" + System.lineSeparator());
        Collection<ConsumerModel> consumerModelList = ApplicationModel.allConsumerModels();
        TTable tTable = new TTable(new TTable.ColumnDefine[] { new TTable.ColumnDefine(TTable.Align.MIDDLE), new TTable.ColumnDefine(TTable.Align.MIDDLE) });
        tTable.addRow("Consumer Service Name", "NUM");
        for (ConsumerModel consumerModel : consumerModelList) {
            tTable.addRow(consumerModel.getServiceName(), getConsumerAddressNum(consumerModel.getServiceName()));
        }
        stringBuilder.append(tTable.rendering());
        return stringBuilder.toString();
    }
}