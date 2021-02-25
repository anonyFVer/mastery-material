package org.apache.dubbo.rpc.protocol.dubbo.telnet;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.telnet.TelnetHandler;
import org.apache.dubbo.remoting.telnet.support.Help;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.protocol.dubbo.DubboProtocol;
import java.util.Collection;

@Activate
@Help(parameter = "[-l] [port]", summary = "Print server ports and connections.", detail = "Print server ports and connections.")
public class PortTelnetHandler implements TelnetHandler {

    @Override
    public String telnet(Channel channel, String message) {
        StringBuilder buf = new StringBuilder();
        String port = null;
        boolean detail = false;
        if (message.length() > 0) {
            String[] parts = message.split("\\s+");
            for (String part : parts) {
                if ("-l".equals(part)) {
                    detail = true;
                } else {
                    if (!StringUtils.isInteger(part)) {
                        return "Illegal port " + part + ", must be integer.";
                    }
                    port = part;
                }
            }
        }
        if (port == null || port.length() == 0) {
            for (ProtocolServer server : DubboProtocol.getDubboProtocol().getServers()) {
                if (buf.length() > 0) {
                    buf.append("\r\n");
                }
                if (detail) {
                    buf.append(server.getUrl().getProtocol()).append("://").append(server.getUrl().getAddress());
                } else {
                    buf.append(server.getUrl().getPort());
                }
            }
        } else {
            int p = Integer.parseInt(port);
            ProtocolServer protocolServer = null;
            for (ProtocolServer s : DubboProtocol.getDubboProtocol().getServers()) {
                if (p == s.getUrl().getPort()) {
                    protocolServer = s;
                    break;
                }
            }
            if (protocolServer != null) {
                ExchangeServer server = (ExchangeServer) protocolServer.getRemotingServer();
                Collection<ExchangeChannel> channels = server.getExchangeChannels();
                for (ExchangeChannel c : channels) {
                    if (buf.length() > 0) {
                        buf.append("\r\n");
                    }
                    if (detail) {
                        buf.append(c.getRemoteAddress()).append(" -> ").append(c.getLocalAddress());
                    } else {
                        buf.append(c.getRemoteAddress());
                    }
                }
            } else {
                buf.append("No such port ").append(port);
            }
        }
        return buf.toString();
    }
}