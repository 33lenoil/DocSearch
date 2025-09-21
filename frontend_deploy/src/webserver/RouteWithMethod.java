package cis5550.webserver;

import java.util.Objects;

public class RouteWithMethod {
    private String route; 
    private String method;  
    
    public RouteWithMethod(String route, String method) {
        this.route = route; 
        this.method = method; 
    }
    
    public String getRoute() {
        return this.route; 
    }
    
    public String getMethod() {
        return this.method; 
    }
    
    
    @Override
    public int hashCode() {
        return Objects.hash(route, method);
    }
    
    
    @Override
    public boolean equals(Object that) {
        
        if (that == this) {
            return true;
        }
        
        if (!(that instanceof RouteWithMethod)) {
            return false;
        }
        
        RouteWithMethod s = (RouteWithMethod) that;
        return this.route == s.route && this.method.equals(s.method);
    }
    
    @Override 
    public String toString() {
        return this.method + " " + this.route.toString(); 
    }
}

