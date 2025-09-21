package cis5550.webserver;

import static cis5550.webserver.Server.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TestServer {
    public static void main(String args[]) throws Exception {
        securePort(443);
        
        get("/", (req, res) -> {return "Hello World - this is Silvia Lang";});

        get("/index/index.html", (req, res) -> {
            String html = readFile("Frontend/index/index.html");
            res.type("text/html");
            res.write(html.getBytes());
            return "OK";
        });

        get("/result/result.html", (req, res) -> {
            String html = readFile("Frontend/result/result.html");
            res.type("text/html");
            res.write(html.getBytes());
            return "OK";
        });

        get("/index/index.js", (req, res) -> {
            String js = readFile("Frontend/index/index.js");
            res.type("text/javascript");
            res.write(js.getBytes());
            return "OK";
        });

        get("/result/result.js", (req, res) -> {
            String js = readFile("Frontend/result/result.js");
            res.type("text/javascript");
            res.write(js.getBytes());
            return "OK";
        });

      }

      public static String readFile(String name) {
          StringBuilder contentBuilder = new StringBuilder();
          try {
              BufferedReader in = new BufferedReader(new FileReader(name));
              String str;
              while ((str = in.readLine()) != null) {
                  contentBuilder.append(str);
              }
              in.close();
          } catch (IOException e) {
          }
          String content = contentBuilder.toString();
          return content;
      }
}
