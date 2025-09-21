package cis5550.webserver;

import java.nio.charset.StandardCharsets;
//import java.io.*;
import java.util.*;


public class HttpRequest {
    public enum Method{GET, HEAD, POST, PUT}; 

    private Method method; 
    private String url ; 
    private Map<String, String> headers; 
    private byte[] body; 
    private String protocol; 
//    private Map<String, String> params;
//    private File file; 
    
    public HttpRequest(Method method, String url) {
        
        this.method = method; 
        this.url = url;
        this.headers = null; 
        this.body = null; 
//        this.params = null; 
//        this.file = null;
        this.protocol = "HTTP/1.1";
    }
    
    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }


    public void setBody(String body) {
        this.body = body.getBytes();
    }
    
    public void setBody(byte[] body) {
        this.body = body;
    }
    
//    public void setFile(File file) {
//        this.file = file;
//    }


//    public void setParams(Map<String, String> params) {
//        this.params = params;
//    }

    public Method getMethod() {
        return method;
    }

    public String getUrl() {
        return url;
    } 
    
    public Map<String, String> getHeaders() {
        return headers;
    } 
    
    public String getProtocol() {
        return protocol;
    } 
    
    public String getBodyStr() {
        return new String(body, StandardCharsets.UTF_8); 
    }
    public byte[] getBody() {
        return body; 
    }
    
    public String toString() {
        String res = this.method.toString() + " " + this.url + " " + this.protocol + "\r\n";
        for (Map.Entry<String, String> header: headers.entrySet()) {
            res += header.getKey() + ": " + header.getValue() + "\r\n";
        }
        res += "\r\n";
        if (this.body != null) {
            res += getBodyStr(); 
        }
        
        return res; 
    }
    
    
    
}
