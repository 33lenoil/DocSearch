package cis5550.webserver;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ResponseImpl implements Response {
    private int statusCode ; 
    private String reasonPhrase; 
    private Map<String, List<String>> headers; 
    private byte[] body; 
    private boolean writeCalled ;
    private OutputStream out; 
    
    public ResponseImpl(OutputStream out) {
        statusCode = 200; 
        reasonPhrase = "OK"; 
        body = "".getBytes(); 
        headers = new HashMap<>(); 
        writeCalled = false; 
        this.out = out; 
    }

    @Override
    public void body(String body) {
        this.body = body.getBytes(); 
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        this.body = bodyArg; 
    }

    /** Add header */
    @Override
    public void header(String name, String value) {
        if(!headers.containsKey(name.toLowerCase())) {
            headers.put(name.toLowerCase(), new ArrayList<String>());
        } 
        headers.get(name.toLowerCase()).add(value); 
    }


    /** Add header Content-Type */
    @Override
    public void type(String contentType) {
        String name = "content-type"; 
        header(name, contentType); 
    }
    

    /** Setter for status and reason*/
    @Override
    public void status(int statusCode, String reasonPhrase) {
        this.statusCode = statusCode; 
        this.reasonPhrase = reasonPhrase; 
    }

    public Map<String, List<String>> getHeaders(){
        return this.headers;
    }
    
    public boolean getWriteCalled() {
        return this.writeCalled;
    }

    
    @Override
    public void write(byte[] b) {
        try {
            if (!writeCalled) {     // first time write() is called 
                writeCalled = true; 
                
                // write status code/reason 
                String firstLine = "HTTP/1.1"+ " " + String.valueOf(statusCode) + " " + this.reasonPhrase + "\r\n"; 
                out.write(firstLine.getBytes());
                out.flush(); 
                
                // write headers 
                for (Map.Entry<String, List<String>> fields : headers.entrySet()) {
                    for (String headerVal : fields.getValue()) {
                        if (fields.getKey().toLowerCase().equals("content-length")) { // don't add Content-Length header 
                            continue; 
                        }
                        String h = fields.getKey() + ": " + headerVal + "\r\n"; 
                        out.write(h.getBytes()); 
                    }
                }
                out.write("Connection: close\r\n".getBytes()); // add connection: close headers 
                out.write("\r\n".getBytes());
                out.flush(); 
            } 
            
            // write body for first and subsequent calls to write()
            out.write(b);
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

    /**
     * Handle logic of sending responses when write() is not called */
    public void respond(Object obj) {
        if (obj != null) { 
            body(obj.toString());  // if return value is not null, overwrite body() 
        } 
        
        String firstLine = "HTTP/1.1"+ " " + String.valueOf(statusCode) + " " + this.reasonPhrase + "\r\n"; 
        try {
            
            out.write(firstLine.getBytes());
            out.flush(); 
            header("Content-Length", String.valueOf(this.body.length)); 
            
            for (Map.Entry<String, List<String>> fields : headers.entrySet()) {
                for (String headerVal : fields.getValue()) {
                    String h = fields.getKey() + ": " + headerVal + "\r\n"; 
                    out.write(h.getBytes()); 
                }
            }
            if (!headers.containsKey("content-type")) {
                String h = "content-type" + ": " + "text/plain" + "\r\n"; 
                out.write(h.getBytes()); 
            }
            
            out.write("\r\n".getBytes());
            out.write(this.body); 
            out.flush(); 
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    @Override
    public void redirect(String url, int responseCode) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        // TODO Auto-generated method stub
        
    }
    
}
