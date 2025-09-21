package cis5550.webserver;

import java.util.*;

public class SessionImpl implements Session {
    private String sessionId; 
    private long creationTime; 
    private long lastAccessedTime;
    private int maxActiveInterval; 
    private Map<String, Object> attributes; 
    private boolean valid ; 
    
    /**
     * Constructor 
     */
    public SessionImpl() {
        this.sessionId = UUID.randomUUID().toString();
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = this.creationTime; 
        this.attributes = new HashMap<>();
        this.maxActiveInterval = 300; 
        this.valid = true; 
    }
    
    /**
     * Copy Constructor 
     */
    public SessionImpl(SessionImpl s) {
        this.sessionId = s.sessionId; 
        this.creationTime = s.creationTime; 
        this.lastAccessedTime = s.lastAccessedTime; 
        this.maxActiveInterval = s.maxActiveInterval; 
        this.attributes = s.attributes; 
        this.valid = s.valid; 
    }

    @Override
    public String id() {
        return sessionId;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        this.maxActiveInterval = seconds; 
    }

    // Invalidates the session. You do not need to delete the cookie on the client when this method
    // is called; it is sufficient if the session object is removed from the server.

    @Override
    public void invalidate() {
        this.valid = false; 
    }

    @Override
    public Object attribute(String name) {
        if (this.attributes != null && this.attributes.containsKey(name)) {
            return attributes.get(name);
        }
        return null;
        
    }

    @Override
    public void attribute(String name, Object value) {
        attributes.put(name, value);
    }
    
    public void setLastAccessedTime(long timestamp) {
        this.lastAccessedTime = timestamp ;
    }
    
    public int getMaxActiveInterval() {
        return this.maxActiveInterval;
    }
    
    public boolean getValid() {
        return this.valid; 
    }
}