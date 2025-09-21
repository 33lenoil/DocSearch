package cis5550.webserver;

import java.util.*;
import java.net.*;
import java.nio.charset.*;

// Provided as part of the framework code

class RequestImpl implements Request {
    private String method;
    private String url;
    private String protocol;
    private InetSocketAddress remoteAddr;
    private Map<String,String> headers;
    private Map<String,String> queryParams;
    private Map<String,String> params;
    private byte bodyRaw[];
    private Server server;
    private String newSessionId ;

    RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg, Map<String,String> queryParamsArg, Map<String,String> paramsArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg) {
        method = methodArg;
        url = urlArg;
        remoteAddr = remoteAddrArg;
        protocol = protocolArg;
        headers = headersArg;
        queryParams = queryParamsArg;
        params = paramsArg;
        bodyRaw = bodyRawArg;
        server = serverArg;
        this.newSessionId = null; 
    }

    public String requestMethod() {
        return method;
    }
    public void setParams(Map<String,String> paramsArg) {
        params = paramsArg;
    }
    public int port() {
        return remoteAddr.getPort();
    }
    public String url() {
        return url;
    }
    public String protocol() {
        return protocol;
    }
    public String contentType() {
        return headers.get("content-type");
    }
    public String ip() {
        return remoteAddr.getAddress().getHostAddress();
    }
    public String body() {
        if (bodyRaw == null) {
            return "";
        }
        return new String(bodyRaw, StandardCharsets.UTF_8);
    }
    public byte[] bodyAsBytes() {
        return bodyRaw;
    }
    public int contentLength() {
        return bodyRaw.length;
    }
    public String headers(String name) {
        return headers.get(name.toLowerCase());
    }
    public Set<String> headers() {
        return headers.keySet();
    }
    public String queryParams(String param) {
        if (this.queryParams == null) {
            return null; 
        }
        return queryParams.get(param);
    }
    public Set<String> queryParams() {
        if (this.queryParams == null) {
            return null;
        }
        return queryParams.keySet();
    }
    public String params(String param) {
        return params.get(param);
    }
    public Map<String,String> params() {
        return params;
    }

    @Override
    public Session session() {
        // if session() called but not already a session, create one and add to server's sessions map 
        if (server.getCurrSession() != null) {
            return server.getCurrSession();
        }
        
        Session s = new SessionImpl(); 
        server.getSessions().put(s.id(), s);
        this.newSessionId = s.id();
        return s;
        
    }
    
    public String getNewSessionId() {
        return this.newSessionId;
    }


}
