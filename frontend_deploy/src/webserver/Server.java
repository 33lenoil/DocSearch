package cis5550.webserver;

import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

import javax.net.*;
import javax.net.ssl.*;

//import cis5550.tools.Logger;

// 992010 
public class Server {
    private static Server server = null; 
    private static boolean serverInited = false; 
    private static int portNum = 80;  // default to 80 before port() is called 
    private static int securePortNo = 0;  // default to 80 before port() is called 
    private static String filePath = null; 
    private static Map<RouteWithMethod, Route> routingTable = new HashMap<>() ; 
    private static Map<String, Session> sessions = new HashMap<>(); 
    private static String currSessionId = null; 
    
    /**
     * Server API Get */
    public static void get(String s, Route r) {
        initServer();
        RouteWithMethod rWithM = new RouteWithMethod(s, "GET");
        routingTable.put(rWithM, r);
    }
    
    /**
     * Server API Post */
    public static void post(String s, Route r) {
        initServer(); 
        RouteWithMethod rWithM = new RouteWithMethod(s, "POST");
        routingTable.put(rWithM, r);
    }
    
    /**
     * Server API put */
    public static void put(String s, Route r) {
        initServer(); 
        RouteWithMethod rWithM = new RouteWithMethod(s, "PUT");
        routingTable.put(rWithM, r);
    }
    
    /**
     * Server API port */
    public static void port(int portNum) {
        if (server == null) {
            server = new Server(); 
        }
        Server.portNum = portNum; 
    }
    
    /**
     * Enable HTTPS requests 
     * */
    public static void securePort(int securePortNo) {
        if (routingTable.size() == 0) {
            Server.securePortNo = securePortNo;
        }
    }    
    
    /**
     * Check if server has ever been initiated. If not, start a new thread that instantiates Server Socket  
     */
    private static void initServer() {
        if (server == null) {
            server = new Server(); 
        }
        if (!serverInited) {    // create singleton pattern instance
            serverInited = true; 
            Thread workerThreadHttps = new Thread(new Runnable() {
                @Override
                public void run() {
                    try { 
                        String pwd = "secret";
                        KeyStore keyStore;
                        keyStore = KeyStore.getInstance("JKS");
                        keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
                        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                        keyManagerFactory.init(keyStore, pwd.toCharArray());
                        SSLContext sslContext = SSLContext.getInstance("TLS");
                        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
                        ServerSocketFactory factory = sslContext.getServerSocketFactory();
                        ServerSocket serverSocketTLS = factory.createServerSocket(securePortNo);
                        
                        serverLoop(serverSocketTLS);
                        
                    } catch (Exception e) {
                        e.printStackTrace();
                    } 
                }
                });
            Thread workerThreadHttp = new Thread(new Runnable() {
                @Override
                public void run() {
                    try { 
                        ServerSocket ssock = new ServerSocket(portNum);
                        serverLoop(ssock);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } 
                }
                });
            if(securePortNo != 0) {
                workerThreadHttps.start(); 
            }
            workerThreadHttp.start(); 
            sessionExpiration(); 
        }
    }
    
    /**
     * Loop for always accepting new connections
     * */
    private static void serverLoop(ServerSocket s) {
        while (true){       
            try {
                Socket sock = s.accept();
                handleClient(sock, filePath); 
            } catch (IOException e) {
                e.printStackTrace();
            } 
            
        }
    }
    
    
    /**
     * Concurrency to handle connection
     */
    private static void handleClient(Socket sock, String filePath) {
        Runnable myRun = new Runnable() {
            @Override
            public void run() {
                HttpConnection conn = new HttpConnection(sock, filePath, server);                    
                conn.handleRequest(); 
                try {
                    sock.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }    
            }
        };
        Thread workerThread = new Thread(myRun);
        workerThread.start(); 
    }
    
    /**
     * Thread to check expiration and delete old sessions */
    public static void sessionExpiration() {
        Runnable myRun = () -> {deleteExpired(); };
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(myRun, 0, 3, TimeUnit.SECONDS);
    }
    
    private static void deleteExpired() {
        Iterator<Map.Entry<String, Session>> iterator = sessions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Session> entry = iterator.next();
            SessionImpl s = (SessionImpl) entry.getValue(); 

            if (s.lastAccessedTime() + 1000 * s.getMaxActiveInterval() < System.currentTimeMillis()) {
                iterator.remove(); // remove expired sessions 
            }
        }
    }
    
    
    /** 
     * Subclass Static Files
     * */
    public static class staticFiles{
        /**
         * Set directory for static files to be delivered 
         * */
        public static void location(String s) {
            if (server == null) {
                server = new Server(); 
            }
            filePath = s; 
        }
    }
    
    /** Getters and setters 
     * */
    public static Map<RouteWithMethod, Route> getRoutingTable(){
        return routingTable;
    }
    
    public static Server getServer() {
        return server; 
    }
    
    public Session getCurrSession() {
        if (currSessionId != null) {
            return sessions.get(currSessionId); 
        }
        return null;
    }
    
    public void setCurrSessionId(String id) {
        currSessionId = id;
    }
    
    public Map<String, Session> getSessions() {
        return sessions; 
    }    
    
}



 


