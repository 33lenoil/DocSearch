package cis5550.webserver;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import cis5550.webserver.HttpRequest.Method;

public class HttpConnection {
    private Socket sock ; 
    private Writer respWriter;
    private InputStream reqReader; 
    private HttpRequest request; 
    private HttpResponse resp; 
    private String filePath; 
    private HttpUtil util; 
    private boolean finished ;
    private Server server ;
    
    /**
     * constructor for creating reader, writer instances 
     */
    public HttpConnection(Socket sock, String filePath, Server server) { 
        
        this.sock = sock;
        this.request = null;
        this.resp = null;
        this.filePath = filePath; 
        this.finished = false; 
        this.server = server; 
        try {
            this.reqReader = sock.getInputStream();
            this.respWriter = new PrintWriter(sock.getOutputStream());
            this.util = new HttpUtil(reqReader); 
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }

    /**
     * Read request and write responses in a loop until there is no more request 
     * */
    public void handleRequest() {
        while (true) {
            
            this.server.setCurrSessionId(null); // reset current session id to null before processing each request 
            processRequest();          // populate this.request 
            
            if (this.finished) {       // stop reading requests when there are no more to read 
                return ;
            }
            if (this.request == null) { // error occurred so request not correctly populated 
                return ; 
            }
            processSession(); 
            
            int res = processDynamic(); // check against dynamic routes
            if (res == 0) {             // continue  to handle the next request 
                continue ; 
            } else if (res == 1) {      // close the connection when write() is called
                break ; 
            } else {               
                processStaticFile();        // handle as static file request when no dynamic route is matched 
            }
            
        }
    } 
    
    /**
     * Parse Cookie header in HTTP request and restore session based on cookie  
     * */
    private void processSession() {
        if (!this.request.getHeaders().containsKey("cookie")) {
            return ;
        }
        
        String cookieInfo = this.request.getHeaders().get("cookie"); 
        String[] kvs = cookieInfo.split(";");
        
        Map<String, String> cookieMap = new HashMap<>(); 
        for (String kv : kvs) {
            String[] keyValue = kv.split("=");
            String key = keyValue[0].trim().toLowerCase(), value = keyValue[1].trim(); 
            cookieMap.put(key, value);
        }
        
        
        if (cookieMap.containsKey("sessionid")) {
            String id = cookieMap.get("sessionid"); 
            if (this.server.getSessions().containsKey(id)){
                SessionImpl s = (SessionImpl) this.server.getSessions().get(id); 
                
                boolean expired = s.lastAccessedTime() + 1000 * s.getMaxActiveInterval() < System.currentTimeMillis();

                // only process the current session & update last accessed time 
                // if not expired and still valid 
                if (!expired && s.getValid()) { 
                    SessionImpl copy = new SessionImpl(s);
                    copy.setLastAccessedTime(System.currentTimeMillis());
                    this.server.getSessions().put(id, copy);
                    this.server.setCurrSessionId(id);
                }
            } 
        }
    }
    
    
    /**
     * Returns true when a route is matched and dynamic content is sent back.
     * Returns false when no route is matched and handled subsequently as a static file request 
     * */
    private int processDynamic() {
//        System.out.println("processing dynamic: " + this.request.getMethod() + " " + this.request.getUrl() + " \r\n" + this.request.getHeaders());
        
        Map<RouteWithMethod, Route> rTable = Server.getRoutingTable();
        for (Map.Entry<RouteWithMethod, Route> entry : rTable.entrySet()) {
            
            if (!entry.getKey().getMethod().toLowerCase().equals(this.request.getMethod().toString().toLowerCase())) { 
                continue;  // if method doesn't match
            }
            
            // if method matches, proceed to check URL pattern 
            String[] tokens = this.request.getUrl().split("\\?"); 
            String urlWithPathParams = this.request.getUrl(), queryParamStr = null; 
            if (tokens.length > 1) {
                urlWithPathParams = tokens[0];
                queryParamStr = tokens[1];
            }
            
            Map<String, String> queryParams = getQueryParams(queryParamStr); 
            Map<String, String> pathParams = HttpUtil.matchPattern(urlWithPathParams, entry.getKey().getRoute()); 
           
            //if URL pattern doesn't match, check the next iteration
            if (pathParams == null) {
                continue;            
            }
            
            // Instantiate req, resp for handler function 
            try {
                
                Request req = new RequestImpl(
                        this.request.getMethod().toString(), urlWithPathParams, this.request.getProtocol(),
                        this.request.getHeaders(), queryParams, 
                        pathParams,  (InetSocketAddress) this.sock.getRemoteSocketAddress(), 
                        this.request.getBody(), Server.getServer()
                      ); 
                Response resp = new ResponseImpl(this.sock.getOutputStream()); 
                Object obj = entry.getValue().handle(req, resp); // pass req, resp to handler function of this route  
                
//                System.out.println(req.requestMethod() + " " + req.url());
//                if (req.params() != null) {
//                    System.out.println(req.params().toString());
//                }
                               
                // if session() has created a new session 
                RequestImpl reqImpl = (RequestImpl) req ; 
                
                
                String sessionIdCreated;
                if ((sessionIdCreated = reqImpl.getNewSessionId()) != null) {
                    resp.header("Set-Cookie", "SessionID=" + sessionIdCreated);
                }
                
                // if write() has been called 
                ResponseImpl respImpl = (ResponseImpl) resp ; 
                
                if (respImpl.getWriteCalled()) {        // ignore return val and body field set before
                    return 1; 
                }
                respImpl.respond(obj);                  // handle the response when write() has not been called 
                 
            } catch (Exception e) {
                e.printStackTrace();
                respError(500, "Internal server error.");
            } 
            return 0; 
        }
        return 2; 
    }
    
    
    /**
     * Return query params extracted from path and body 
     * */
    private Map<String, String> getQueryParams(String queryParamStr){
        Map<String, String> queryParams = new HashMap<>(); 
        String queryParamStrConcat = ""; 
        
        // get query parameters from path
        if (queryParamStr != null) {
            queryParamStrConcat += queryParamStr;
        }
        
        // get query parameters from body 
        if (this.request.getHeaders().containsKey("content-type") && 
                this.request.getHeaders().get("content-type").equals("application/x-www-form-urlencoded")) {
            if (queryParamStrConcat.length() > 0) {
                queryParamStrConcat += "&";
            }
            queryParamStrConcat += new String(this.request.getBody(), StandardCharsets.UTF_8); 
        }
        
        if (queryParamStrConcat.length() == 0) {
            return null; 
        }
        
        // get params and put into result map 
        
        String[] params = queryParamStrConcat.split("\\&"); 
        for (String param : params) {
            
            try {
                String[] kv = param.split("=");
                if (kv.length == 1) {
//                    System.out.println("empty query param");
                    queryParams.put(kv[0].strip(), ""); 
                } else if (kv.length == 2) {
                    queryParams.put(kv[0].strip(), URLDecoder.decode(kv[1].strip(), "UTF-8"));
                }
            } catch (Exception e) {
                e.printStackTrace();
                continue; 
            }
        }
        return queryParams; 
    }
    
    
    /**
     * When none of the dynamic routes is matched, process as Dynamic File 
     * */
    private void processStaticFile() {
        if (processStaticFileError()) { // errors caught in processStaticFileError
//            this.finished = true; 
            return ; 
        }
        
        // no error in request, respond with requested file 
        File f = new File(this.filePath + "//" + this.request.getUrl()); 
        processFileResponse(200, f); 
    }
    
    /**
     * Read request and populate request field. Handle errors incurred by request  
     * */
    private void processRequest() {
        // read first line 
        String reqStr = this.util.readUtil(2, "[13, 10]"); 
//        System.out.println("server received request: " + reqStr);
        if (reqStr.length() == 0) {
            this.finished = true; 
            return ; 
        } 
        
        if (processReqError(reqStr)) { // errors caught in processReqError
//            System.out.println("Error in request line: " +  reqStr);
            this.finished = true; 
            return ; 
        }
        
        String[] tokens = reqStr.split(" ");
        String method = tokens[0].trim(), url = tokens[1].trim();
        
        // populate fields 
        Method m = HttpUtil.getMethod(method);
        this.request = new HttpRequest(m, url); 
        
        this.request.setHeaders(this.util.processReqHeaders());
        if (processReqHeaderError()) { // errors caught in processReqHeaderError
            System.out.println("Error in request header: " +  reqStr);
            return ; 
        }
        
        // check connection closed headers 
        if (this.request.getHeaders().containsKey("Connection") && 
                this.request.getHeaders().get("Connection").toLowerCase().equals("close")) {
//            System.out.println(this.request.getHeaders());
            this.finished = true; 
        }
        // check content length and read body accordingly 
        String lenHeader = "content-length";
        if (this.request.getHeaders().containsKey(lenHeader)) {
            int contentLen = 0; 
            try {
                contentLen = Integer.valueOf(this.request.getHeaders().get(lenHeader).trim()); 
            } catch(NumberFormatException e) {
                e.printStackTrace();
                return ; 
            }
            if (contentLen > 0) {
                byte[] body = processReqBody(contentLen); 
                this.request.setBody(body);  
            }
        } 
    }

    

    /**
     * Read request body
     */
    private byte[] processReqBody(int contentLen) {
        byte[] res = new byte[contentLen];
        
        int i = 0; 
        while (contentLen > 0) { // exit when finish reading length specified in header 
            
            try {
                res[i] = (byte) this.reqReader.read();
                i++; 
            } catch (IOException e) {
                e.printStackTrace();
            } 
            contentLen --; 
        }
                
        
        return res; 
        
    }
    
    
    
    /**
     * Write response 
     * */
    private void processFileResponse(int responseCode, File f) {
    
        try (FileInputStream fr = new FileInputStream(f)){
            
            this.resp = new HttpResponse(responseCode); 
            Map<String, String> respHeaders = new HashMap<>(); 
            
            // populate correct file type based on extension of requested file 
            String ext = "", type = ""; 
            int lastIndex = this.request.getUrl().lastIndexOf(".");
            if (lastIndex != -1) {
                ext = this.request.getUrl().substring(lastIndex + 1).toLowerCase();
            }
            if (ext.equals("jpg")|| ext.equals("jpeg")) {
                type = "image/jpeg";
            } else if (ext.equals("txt")) {
                type = "text/plain";
            } else if (ext.equals("html")) {
                type = "text/html";
            } else {
                type = "application/octet-stream";
            }
            
            // write headers 
            respHeaders.put("Content-Type", type);
            respHeaders.put("Server", "Silvia's Server");
            respHeaders.put("Content-Length", String.valueOf(f.length()));
            resp.setHeaders(respHeaders);
            
            this.respWriter.write(resp.headerToString());
            this.respWriter.flush();  
            
            if (this.request.getMethod() == Method.HEAD) { // if HEAD, only send headers
                fr.close(); 
                return ; 
            }
           
            // write requested file 
            byte[] buffer = new byte[4096];
            int currRead = 0; 
            OutputStream out = sock.getOutputStream();
            while ((currRead = fr.read(buffer)) != -1) {
                out.write(buffer, 0, currRead);
                out.flush();
            }
            
            fr.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }
    
    /**
     * Return error codes for errors caught in the first line of request 
     * */
    private boolean processReqError(String reqLine) {
        String[] tokens = reqLine.split(" ");
        if (tokens.length < 3) {
            respError(400, "Method, URL, or HTTP protocol missing");            
            return true; 
        }
        
        String method = tokens[0].trim(), url = tokens[1].trim(), protocol = tokens[2].trim();
        if (!protocol.equals("HTTP/1.1")) {
            respError(505, "HTTP version not supported");
            return true;
        }
        
        Method m = HttpUtil.getMethod(method);
        if (m == null) {
            respError(501, "HTTP method not implemented");
            return true;
        }
        
        if (url.contains("..")) {
            respError(403, "Requested file is forbidden");
            return true; 
        }
        
        return false;
    }
    
    
    private boolean processStaticFileError() {
        File f = new File(this.filePath + this.request.getUrl()); 
        if (!f.exists()) {
//            System.out.println("404");
            respError(404, "Requested file does not exist");
            return true; 
        }
        if (!f.canRead()) {
            respError(403, "Requested file exists but is not readable");
            return true; 
        }
        return false; 
    }
    
    
    /**
     * Return error codes for errors caught in the request header 
     * */
    private boolean processReqHeaderError() {
        if (!this.request.getHeaders().containsKey("host")) {
            respError(400, "Host missing");
            return true; 
        }
        return false; 
    }
    
    /**
     * Write errors to response 
     * */
    private void respError(int responseCode, String body) {
        resp = new HttpResponse(responseCode); 
        Map<String, String> respHeaders = new HashMap<>(); 
        respHeaders.put("Content-Type", "text/plain");
        respHeaders.put("Server", "Silvia's Server");
        int len = body.length(); 
        respHeaders.put("Content-Length", String.valueOf(len));
        resp.setHeaders(respHeaders);
        resp.setBody(body);
        try {
            this.respWriter.write(resp.toString());
            this.respWriter.flush();  
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
}