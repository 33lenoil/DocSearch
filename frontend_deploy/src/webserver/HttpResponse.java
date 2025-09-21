package cis5550.webserver;

import java.util.*;

public class HttpResponse {
    private int code; 
    private String message; 
    private Map<String, String> headers; 
    private String body; 
    private static String protocol = "HTTP/1.1";
    
    private static Map<Integer, String> messages = new HashMap<>(); 
    static {
        messages.put(200, "OK");
        messages.put(400, "Bad Request");
        messages.put(403, "Forbidden");
        messages.put(404, "Not Found");
        messages.put(405, "Not Allowed");
        messages.put(500, "Internal Server Error");
        messages.put(501, "Not Implemented");
        messages.put(505, "HTTP Version Not Supported");
    }
    
    
    public HttpResponse(int code) {
        this.code = code; 
        this.message = messages.get(this.code); 
        this.headers = new HashMap<String, String>(); 
        this.body = ""; 
    }
    
    public String toString() {
        String res = "";
        res += protocol + " " + Integer.toString(this.code) + " " +  this.message + "\r\n";
        for (Map.Entry<String, String> header : this.headers.entrySet()) {
            res += header.getKey() + ": " + header.getValue() + "\r\n"; 
        }
        res += "\r\n"; 
        res += this.body; 
        return res; 
    }
    
    public String headerToString() {
        String res = "";
        res += protocol + " " + Integer.toString(this.code) + " " +  this.message + "\r\n";
        for (Map.Entry<String, String> header : this.headers.entrySet()) {
            res += header.getKey() + ": " + header.getValue() + "\r\n"; 
        }
        res += "\r\n"; 
        return res; 
    }
    
    
    public Map<String, String> getHeaders() {
        return headers;
    }


    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }


    public String getBody() {
        return body;
    }


    public void setBody(String body) {
        this.body = body;
    }


    public int getCode() {
        return code;
    }


    public String getMessage() {
        return message;
    }


    
    
    
}
