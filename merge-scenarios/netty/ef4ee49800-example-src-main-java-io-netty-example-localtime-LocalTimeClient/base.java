package io.netty.example.localtime;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import io.netty.bootstrap.ClientBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.nio.NioClientSocketChannelFactory;

public class LocalTimeClient {

    private final String host;

    private final int port;

    private final Collection<String> cities;

    public LocalTimeClient(String host, int port, Collection<String> cities) {
        this.host = host;
        this.port = port;
        this.cities = new ArrayList<String>();
        this.cities.addAll(cities);
    }

    public void run() {
        ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new LocalTimeClientPipelineFactory());
        ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(host, port));
        Channel channel = connectFuture.awaitUninterruptibly().getChannel();
        LocalTimeClientHandler handler = channel.getPipeline().get(LocalTimeClientHandler.class);
        List<String> response = handler.getLocalTimes(cities);
        channel.close().awaitUninterruptibly();
        bootstrap.releaseExternalResources();
        Iterator<String> i1 = cities.iterator();
        Iterator<String> i2 = response.iterator();
        while (i1.hasNext()) {
            System.out.format("%28s: %s%n", i1.next(), i2.next());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            printUsage();
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        Collection<String> cities = parseCities(args, 2);
        if (cities == null) {
            return;
        }
        new LocalTimeClient(host, port, cities).run();
    }

    private static void printUsage() {
        System.err.println("Usage: " + LocalTimeClient.class.getSimpleName() + " <host> <port> <continent/city_name> ...");
        System.err.println("Example: " + LocalTimeClient.class.getSimpleName() + " localhost 8080 America/New_York Asia/Seoul");
    }

    private static List<String> parseCities(String[] args, int offset) {
        List<String> cities = new ArrayList<String>();
        for (int i = offset; i < args.length; i++) {
            if (!args[i].matches("^[_A-Za-z]+/[_A-Za-z]+$")) {
                System.err.println("Syntax error: '" + args[i] + "'");
                printUsage();
                return null;
            }
            cities.add(args[i].trim());
        }
        return cities;
    }
}