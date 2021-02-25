package io.netty.example.http.snoop;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;
import io.netty.bootstrap.ClientBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.nio.NioClientSocketChannelFactory;
import io.netty.handler.codec.http.CookieEncoder;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

public class HttpSnoopClient {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(HttpSnoopClient.class);

    private final URI uri;

    public HttpSnoopClient(URI uri) {
        this.uri = uri;
    }

    public void run() {
        String scheme = uri.getScheme() == null ? "http" : uri.getScheme();
        String host = uri.getHost() == null ? "localhost" : uri.getHost();
        int port = uri.getPort();
        if (port == -1) {
            if (scheme.equalsIgnoreCase("http")) {
                port = 80;
            } else if (scheme.equalsIgnoreCase("https")) {
                port = 443;
            }
        }
        if (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https")) {
            logger.error("Only HTTP(S) is supported.");
            return;
        }
        boolean ssl = scheme.equalsIgnoreCase("https");
        ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new HttpSnoopClientPipelineFactory(ssl));
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
        Channel channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess()) {
            future.getCause().printStackTrace();
            bootstrap.releaseExternalResources();
            return;
        }
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath());
        request.setHeader(HttpHeaders.Names.HOST, host);
        request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
        CookieEncoder httpCookieEncoder = new CookieEncoder(false);
        httpCookieEncoder.addCookie("my-cookie", "foo");
        httpCookieEncoder.addCookie("another-cookie", "bar");
        request.setHeader(HttpHeaders.Names.COOKIE, httpCookieEncoder.encode());
        channel.write(request);
        channel.getCloseFuture().awaitUninterruptibly();
        bootstrap.releaseExternalResources();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            logger.error("Usage: " + HttpSnoopClient.class.getSimpleName() + " <URL>");
            return;
        }
        URI uri = new URI(args[0]);
        new HttpSnoopClient(uri).run();
    }
}