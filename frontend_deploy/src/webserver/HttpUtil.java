package cis5550.webserver;

import java.io.*;
import java.util.*;

import cis5550.webserver.HttpRequest.Method;

public class HttpUtil {
    private InputStream r; 
    
    public HttpUtil(InputStream reqReader) {
        this.r = reqReader; 
    }
    
    public String readUtil(int numChar, String endStr) {
        List<Integer> req = new ArrayList<Integer>(); 
        int currByte = 0;
        try {
            while (true) { 
                currByte = r.read();
                
                if (currByte == -1) { // EOF reached 
                    break; 
                }
                req.add(currByte); 
                if (req.size() < numChar){
                    continue; 
                }
                 
                // check end of header
                List<Integer> last = req.subList(req.size() - numChar, req.size());
                if (last.toString().equals(endStr)) {
                    break; 
                }
            }
        } catch (IOException e) {
            System.out.println("Reading request exception");
            e.printStackTrace();
        }
        
        StringBuilder str = new StringBuilder();
        for (int reqByte : req) {
            str.append((char) reqByte);
        }
        return str.toString();
    }
    
    public static Method getMethod(String method) {
        Method m = null;
        switch(method){
            case "GET":
                m = Method.GET; 
                break;
            case "HEAD":
                m = Method.HEAD; 
                break;
            case "POST":
                m = Method.POST; 
                break;
            case "PUT":
                m = Method.PUT; 
                break;
            default: 
                break; 
        }
        return m;
    }
    
    /**
     * Read request headers
     */
    public Map<String, String> processReqHeaders() {
        
        String headerStr = this.readUtil(4, "[13, 10, 13, 10]"); 
        
        Map<String, String> res = new HashMap<>(); 
        String[] headers = headerStr.split("\r\n"); 
        
        for (int i = 0; i < headers.length; i++) {
            String header = headers[i];
            
            String[] tokens = header.split(":", 2);
            res.put(tokens[0].toLowerCase().trim(), tokens[1].trim()); 
        }
        return res; 
        
    }    
    
    /**
     * Given a URL and current iteration of the routing table route pattern, return whether the URL matches the route pattern 
     * */
    public static Map<String, String> matchPattern(String url, String route) {
        String[] urlParts = url.split("/");
        String[] routeParts = route.split("/");
        
        if (urlParts.length != routeParts.length) {
            return null; 
        }
        Map<String, String> res = new HashMap<>(); 
        for (int i = 0; i < routeParts.length; i++) {
            if (routeParts[i].length() == 0) {
                continue; 
            }
            
            if (!routeParts[i].equals(urlParts[i]) && routeParts[i].charAt(0) != ':') {
                return null; 
            }
            if (routeParts[i].charAt(0) == ':') {
                res.put(routeParts[i].substring(1), urlParts[i]);
            } else {
                res.put(routeParts[i], urlParts[i]);
            }
        }
        return res; 
    }
}
