package cis5550.jobs;


import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import java.util.zip.DeflaterOutputStream;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools1.*;


public class Crawler {
    private static final String HOSTS = "hosts";  // for rules associated with each host 
    private static final String CRAWLED = "pt-crawl"; // final crawled pages data
    private static final String DOMAINS = "domains"; // for keeping track of domains visited & exporting diagnostics table 
    private static final String VISITED = "visited"; // visited set with just URLs
    private static final double MAX_VISITED = 4 * Math.pow(10, 5); // 1 * Math.pow(10, 6)
    private static final double MAX_CRAWLED = 4 * Math.pow(10, 5); // 
    private static final int PREFIX_NUM = 5; 
    
    private static String prefix(String url) {
        return Hasher.hash(url).substring(0, PREFIX_NUM);
    }
    
    private static void commonWords(KVSClient kvs) throws IOException{
        Set<String> res = new HashSet<>(); 
        try {
            BufferedReader in = new BufferedReader(new FileReader(new File("common_words.txt")));
            String word = ""; 
            while((word = in.readLine()) != null) {
                res.add(word); 
            }
        } catch (Exception e) {
            e.printStackTrace();
        } 
        
        for (String word: res) {
            kvs.put("CommonWords", Hasher.hash(word), "word", word.toLowerCase());
        }
        
    }
    
    /**
     * escape is null or "Barak_Obama"*/
    public static String stripPage(String page, KVSClient kvs, String escape) throws FileNotFoundException, IOException {
        Set<String> escapeSet = new HashSet<>(); 
        if (escape != null){
            String[] escapes = escape.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase().split("_");
            for (String escapeStr: escapes) {
                escapeSet.add(escapeStr); 
                if (escapeStr.length() < 15) {
//                    System.out.println(escapeStr);
                    kvs.put("CommonWords", Hasher.hash(escapeStr), "word", escapeStr);  
                }
                        
            }
        }
        
        String[] words = page.replaceAll("<[^>]*>", " ").replaceAll("[^a-zA-Z0-9]", " ").
                replaceAll("<style>([\\s\\S]*?)</style>", " ").replaceAll("<script>([\\s\\S]*?)</script>", " ").
                toLowerCase().trim().split("\\s+");
        for (int i = 0; i < words.length; i++) {
            if (!kvs.existsRow("CommonWords", Hasher.hash(words[i])) && !escapeSet.contains(words[i])) {
                words[i] = ""; 
            };
        } 
        String res = String.join(" ", words); 
        return res; 
    }
   
    private static String extractTitle(String page) {
        String titlePattern = "<title>([\\s\\S]*?)</title>";
        Pattern pattern = Pattern.compile(titlePattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(page);

        if (matcher.find()) {
            return matcher.group(1).replaceAll("<title>", "").replaceAll("</title>", "");
        } else {
            return null; 
        }
    }
    
    /**
     * @param str: the HTML string to extract anchor tags from 
     * @param url: the URL associated with this current HTML page 
     * @param kvsCoordinator */
    public static List<String> addNewLinks(String str, String url, String kvsCoord) throws Exception {
        KVSClient kvs = new KVSClient(kvsCoord); 
        
        List<String> toExplore = extractURL(str); 
        List<String> res = new ArrayList<>() ;
        
        // for each link found, normalize it and enque, if applicable 
        for (String newLink : toExplore) {
            
            String normalized = normalizeURL(url, newLink);            
            
            if (normalized == null || kvs.existsRow(VISITED, Hasher.hash(normalized))) { // not a valid URL to explore 
                continue; 
            }
            
            String[] normalizedParts = URLParser.parseURL(normalized);
            if (normalizedParts != null && normalizedParts[1] == null) {
                continue ;
            }
            
            if (prep(kvs, normalizedParts, normalized) == null) { // check for exclusion with prep()
                continue;
            }
            
            if (normalizedParts[1].contains("espn.com.")) { // don't enque foreign language ESPN 
                continue; 
            }
            
         // filter out URLs that are more than 4 steps down 
            String path = normalizedParts[3]; 
            String[] pathParts = path.split("/");
            if (pathParts.length > 3) {
                continue ; 
            }
            
            // filter out wikipedia pages that are not english 
            if (normalizedParts[1].contains("wikipedia.org") && !normalizedParts[1].contains("en.wikipedia.org")) { // if wikipedia but not en.wikipedia 
                continue; 
            }
            
            // filter out mailto 
            if (normalized.contains("mailto:")) {
                continue; 
            }
            
            
            String hashedHost = Hasher.hash(normalizedParts[1]);
            byte[] nPagesBytes = kvs.get(DOMAINS, hashedHost, "nPages"); 
            int nPages = 0; 
            if (nPagesBytes != null) {
                String nPagesStr = new String(nPagesBytes, StandardCharsets.UTF_8); 
                nPages = Integer.valueOf(nPagesStr); 
            }
            
            if (normalizedParts[1].contains("wikipedia.org") && !normalizedParts[1].contains("en.wikipedia.org")) {
                continue; 
            }
            
            // TODO: change this to check seed domain 
            String normalizedDomain = normalizedParts[1];
            String newPriority = "";
            if (kvs.existsRow("seed-domain", Hasher.hash(prefix(normalizedDomain) + normalizedDomain))) {
                if (nPages > 10000) {
                    continue; 
                } 
                if (nPages <= 1000) {
                    newPriority = "1";
                } else if (nPages < 3000) {
                    newPriority = "2"; 
                } else {
                    newPriority = "3";
                }
            } else {
                if (nPages > 50) {
                    continue; 
                }
                if (nPages <= 10) {
                    newPriority = "1"; 
                } else if (nPages > 10 && nPages <= 25) {
                    newPriority = "2"; 
                } else {
                    newPriority = "3"; 
                }
            }
            
            // enque -- updates VISITED and 
            enque(kvs, normalized);
            res.add(prefix(normalized) + newPriority + " " + normalized); //  add to queue 
        }
        return res;
    }
    
    
    /** every time an url in enqued, need to update VISITED table and DOMAINS table  
     * @throws Exception */
    public static void enque(KVSClient kvs, String normalized) throws Exception {
        
        
        String host = URLParser.parseURL(normalized)[1];
        String hashedURL = Hasher.hash(normalized); 
        String hashedHost = Hasher.hash(host); 
        
        if (kvs.existsRow(VISITED, hashedURL)) {
            return ;
        }
        
        kvs.put(VISITED, hashedURL, "url", normalized);
        
        
        if (!kvs.existsRow(DOMAINS, hashedHost)){ // not row for this domain yet 
            Row r = new Row(hashedHost); 
            r.put("host", host);
            r.put("nPages", "1");
            kvs.putRow(DOMAINS, r);
        } else {                                           // already row for this domain 
            String nPagesStr = new String(kvs.get(DOMAINS, hashedHost, "nPages"), StandardCharsets.UTF_8); 
            int nPages = Integer.valueOf(nPagesStr); 
            kvs.put(DOMAINS, hashedHost, "nPages", String.valueOf(1+nPages));
        }
        return ;
    }
    

    /** Method to be run in the Flame job submitted */
    public static void run(FlameContext ctx, String[] args) throws Exception {
        
        if (args.length == 1 || args.length == 2) {
            // args[0] is seed url 
            // args[1] is arg to continue from frontier 
            ctx.output("OK");
        } else {
            ctx.output("Error: please provide one seed url");
        }
        
        FlameRDD urlQueue = null; 
        if (args.length == 1) {
            String[] seeds = args[0].split(",");
            List<String> urls = new ArrayList<>(); 
            for (String seed: seeds) {
                String normalized = normalizeSeedURL(seed); 
                
                urls.add(prefix(normalized) + "1 " + normalized); // assign priority 1 -- first priority 
                
                KVSClient kvs = ctx.getKVS(); 
                enque(kvs, normalized);
                
                // add a table for seed domain. If in seed domain, upper limit is 5000 
                String domain = URLParser.parseURL(normalized)[1];
                kvs.put("seed-domain", Hasher.hash(prefix(domain) + domain), "domain", domain);
            }
            
            urlQueue = ctx.parallelize(urls);
            commonWords(ctx.getKVS()); // put common words in kvs
            
        } else if (args.length == 2 && args[1].equals("continue")) {
            urlQueue = ctx.fromTable("pt-frontier", row -> {return row.get("value");}); 
        }
        
        
        String kvsCoordArg = ctx.getKVS().getCoordinator(); 
        Set<Integer> redirects = new HashSet<>(new ArrayList<>(Arrays.asList(301, 302, 303, 307, 308)));

        int i = 0; 
        
        boolean[] flag = { false };
        // queue of links to crawl 
        while (urlQueue.count() != 0) {
            // flatMap maps a single URL to a collection of URLs to be explored 
            
            
            try {
                
                FlameRDD newQueue = urlQueue.flatMap(orig -> {
                    try {
                        System.out.println(System.currentTimeMillis());
                        
                        String input = orig.substring(PREFIX_NUM); 
                        KVSClient kvs = new KVSClient(kvsCoordArg); 
                        
                        String[] tokens = input.split(" "); 
                        String priority = tokens[0], url = tokens[1];
                        
                        String[] urlParts = URLParser.parseURL(url);
                        if (priority.equals("2")) {
                            return new ArrayList<String>(Arrays.asList("1 " + url)); 
                        }
                        
                        if (priority.equals("3")) {
                            byte[] nPages = kvs.get("domains", Hasher.hash(urlParts[1]), "nPages"); 
                            if (nPages != null && Integer.valueOf(new String(nPages,StandardCharsets.UTF_8)) > 100 ) {
                                return new ArrayList<String>();         // deque if not enough room
                            } else {
                                return new ArrayList<String>(Arrays.asList(prefix(url) + "3 " + url)); // put back to queue 
                            }
                        }
                        
                        
                        String rowKey = Hasher.hash(urlParts[1]);  // host hashed
                        String key = Hasher.hash(url);  // url hashed 
                       
                        String resStr = prep(kvs, urlParts, url); // filter out not enough time, excluded etc 
                        
                        if (resStr == null) {
                            return new ArrayList<String>(); 
                        } else if (resStr.length() == 0) {
                            return new ArrayList<String>(Arrays.asList("3 " + url)); 
                        } 

                        // HEAD request 
                        URL urlConn = new URL(url);
                        HttpURLConnection conn = (HttpURLConnection) urlConn.openConnection();
                        conn.setRequestMethod("HEAD");
                        conn.setRequestProperty("User-Agent", "cis5550-crawler");
                        conn.setConnectTimeout(1000);
                        conn.setReadTimeout(3000);
                        conn.setInstanceFollowRedirects(false);
                       
                        if (conn.getHeaderField("Content-Language")!=null && // filter out non-English pages 
                                !conn.getHeaderField("Content-Language").contains("en")) {
                            return new ArrayList<String>();
                        }
                        
                        long len = conn.getContentLengthLong(); 
                        if (len > Math.pow(10, 7)) {  // filter out pages greater than 10MB
                            return new ArrayList<String>();
                        }
                        
                            // before sending request, update the "hosts" records
                        
                        kvs.put(HOSTS, rowKey, "lastAccessed", String.valueOf(System.currentTimeMillis()));
                        
                            // send request and get response code  
                        int respStatus = conn.getResponseCode(); 
                        Row row = new Row(Hasher.hash(prefix(url) + url));
                        
                        row.put("url", url);
//                        row.put("responseCode", String.valueOf(respStatus));
                        
                            // add content-length and type 
                        if (len != -1) {
//                            row.put("length", String.valueOf(len));
                        } 
                        
                        String type = conn.getContentType(); 
                        if (type != null) {
//                            row.put("contentType", type);
                        }
                        
                        if (conn.getHeaderField("Content-Language")!=null && // filter out non-English pages 
                                !conn.getHeaderField("Content-Language").contains("en")) {
                            return null; 
                        }
                            // if 200, make another GET request 
                        if (respStatus == 200) { 
                            
                            HttpURLConnection connGet = (HttpURLConnection) new URL(url).openConnection();
                            connGet.setRequestMethod("GET");
                            connGet.setRequestProperty("User-Agent", "cis5550-crawler");
                            connGet.setConnectTimeout(1000);
                            connGet.setReadTimeout(3000);
                            connGet.connect(); 
                            
                            int respStatus2 = connGet.getResponseCode(); 
//                            row.put("responseCode", String.valueOf(respStatus2)); // overwrite HEAD resp status 
                            
                            if (respStatus2 != 200 || !type.toLowerCase().contains("text/html")) { // GET resp status 
                                kvs.putRow(CRAWLED, row);
                                return new ArrayList<String>();
                            }
                            
                            // download page content from text/html and extract links 
                            byte[] page = connGet.getInputStream().readAllBytes();      
                            String str = new String(page, StandardCharsets.UTF_8); 
                            if (!str.contains("lang=\"en") && !str.contains("lang=en") && !urlParts[1].contains("www.goodreads.com"))  { // filter out foreign language html 
                                kvs.put("no-crawl-url", key, "url", url);
                                return new ArrayList<String>(); 
                            } 
                            
                            
                            String escape2 = null; 
                            
                            if (kvs.existsRow("seed-domain", Hasher.hash(prefix(urlParts[1]) + urlParts[1]))) {
                                String[] urlDeep = urlParts[3].split("/"); 
                                escape2 = urlDeep[urlDeep.length - 1];
                            }
                            
                            String stripped2 = stripPage(str, kvs, escape2);
                            byte[] compressed2 = compress(stripped2.getBytes()); 
                            row.put("page", compressed2);
                            row.put("title", extractTitle(str));
                            kvs.putRow(CRAWLED, row);
                            
                            if (!flag[0]) {
                                try {
                                    List<String> links = addNewLinks(str, url, kvsCoordArg);
                                    
                                    Row newR = new Row(Hasher.hash(prefix(url)+url));
                                    newR.put("url", url);
                                    
                                    List<String> trimmedLinks = new ArrayList<>(); 
                                    
                                    for (String link: links) {
                                        trimmedLinks.add(link.substring(PREFIX_NUM + 2));
                                    }
                                    String linkTo = String.join(",", trimmedLinks);
                                    newR.put("LinkTo", linkTo);
                                    kvs.putRow("pt-url", newR);
                                    
                                    return links;
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            } 
                            
                        } else if (redirects.contains(respStatus)) { // redirects 
                            String loc = conn.getHeaderField("Location"); 
                            kvs.putRow(CRAWLED, row);
                            String normalized = normalizeURL(url, loc); 
                            
                            if (normalized == null || kvs.existsRow("visited", Hasher.hash(normalized))) {
                                return new ArrayList<String>(); 
                            }
                            
                            return new ArrayList<String>(Arrays.asList(prefix(normalized) + "1 " + normalized)); 
                        }                     
                        
                        kvs.putRow(CRAWLED, row);
                        return new ArrayList<String>();   // not 200 or redirect
                    } catch(Exception e) {
                        return new ArrayList<String>();   // not 200 or redirect
                    }
                 });
                
                // update url queue 
                
                urlQueue = newQueue; 
                KVSClient kvs = new KVSClient(kvsCoordArg); 
                if (i != 0) {
                    kvs.delete("pt-frontier");
                }
                newQueue.saveAsTable("pt-frontier");
                i ++; 
                if (kvs.count("pt-crawl") > MAX_CRAWLED) { // if more than 1M pages crawled, break
                    Iterator<Row> iter = kvs.scan(DOMAINS);
                    OutputStream out = new FileOutputStream(new File("domains.csv")); 
                    while (iter.hasNext()) {
                        Row r = iter.next();
                        String domainName = r.get("host");
                        String counts = r.get("nPages"); 
                        out.write((domainName + "," + counts + "\r\n").getBytes());
                        out.flush(); 
                    }
                    break;
                }
                if (kvs.count(VISITED) > MAX_CRAWLED) { // if more than 1M pages visited, don't enque anymore 
                    flag[0] = true;
                    System.out.println("Finished exploring");
                }
                
                
            } catch (Exception e) {
                e.printStackTrace();
                continue; 
            }
            
//            Thread.sleep(500); // for rate limiting, sleep for 1 sec 
        }
    }
    
    /**
     * Parse HTML and search for href property inside anchor tags */
    public static List<String> extractURL(String html) {        
        List<String> res = new ArrayList<>(); 
        String regex = "<a[^>]*>[\\s\\S]*?<\\/a>"; // this matches everything between '<a' and '</a>'

        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(html);
        
        
        // Extract content between <a> and </a>
        while (matcher.find()) {
            try {
                String content = matcher.group();
                
                String regex2 = "href=(\"|')(.*?)(\"|')"; // matches everything between href=" and "
                Pattern pattern2 = Pattern.compile(regex2, Pattern.CASE_INSENSITIVE);
                Matcher matcher2 = pattern2.matcher(content);
                while(matcher2.find()) {
                    String link = matcher2.group(); 
                    
                    if (link.contains("\"")) {
                        
                        String[] tokens = link.split("\"");
                        res.add(tokens[1]); 
                    } else if (link.contains("\'")){
                        String[] tokens = link.split("\"");
                        res.add(tokens[1]); 
                    }
                } 
            } catch (Exception e) {
                continue; 
            }
            
        }
        return res; 
    }
    
    /** Handle partial and relative links */
    public static String normalizeURL(String base, String url) {
        String[] bParts = URLParser.parseURL(base); 
         
        // case 1: handle # 
        int i = url.indexOf('#');
        if (i != -1) {
            
            url = url.substring(0, i);
            
            if (url.length() == 0) { // empty after cutting off the part after # 
                return null;  // discard 
            }
        }
        
        // case 2: relative link
            // forward link doesn't start with any slash, like "blah.html"
            // backward link starts with "../"
        String[] currParts = URLParser.parseURL(url);
        
        if (currParts[0] == null && currParts[3].charAt(0) != '/') { 
            
            // protocol missing -- relative link 
            i = base.lastIndexOf('/');
            String baseBefore = base; 
            if (i != -1) {
                baseBefore = base.substring(0, i);
            }
            
            if (url.charAt(0) != '.') { // forward link 
                url = baseBefore + "/" + url;
                return url; 
            } else if (url.length() >= 4 && url.substring(0, 3).equals("../")) { // backward link 
                while (url.substring(0, 3).equals("../")) {
                    url = url.substring(3, url.length()); 
                    i = baseBefore.lastIndexOf('/');
                    baseBefore = baseBefore.substring(0, i); 
                }
                url = baseBefore  + "/" + url; 
                return url ;
            }
            
        } 
        
        // case 3: host and port missing, but url starts with "/" -- absolute link 
        
            // if host is present but port missing
//        System.out.println(currParts[0] + " " + currParts[1] + " " + currParts[2]  +  " " + currParts[3]); 
        
        if (currParts[1] != null && currParts[2] == null) {
            if (currParts[0].equals("http")) {
                currParts[2] = "80";
            }
            if (currParts[0].equals("https")) {
                currParts[2] = "443";
            }
        } else if (currParts[1] == null) {
            
            // host missing, fill with base's protocol, host, and port
            for (int j = 0; j <= 1; j++) {
                if (currParts[j] == null) {
                    currParts[j] = bParts[j]; 
                }
            }
            // handle the case where no port is present in base
            if (bParts[2] != null) {
                currParts[2] = bParts[2]; 
            } else {
                currParts[2] = (currParts[0].equals("http")) ? "80" : "443";
            }
        }
            // generate new URL based on parts completed 
        url = currParts[0] + "://" + currParts[1] + ":" + currParts[2]  + currParts[3]; 
        
        
        // case 4: filter based on protocol and file extension 
        if (!currParts[0].equals("http") && !currParts[0].equals("https")) {
            return null; 
        }
        
        i = url.lastIndexOf('.');
        List<String> arrayList = new ArrayList<>(Arrays.asList("jpg", "jpeg", "gif", "png", "txt"));
        Set<String> invalidExt = new HashSet<>(arrayList); 
        
        if (i != -1) {
            String ext = url.substring(i + 1, url.length());
            if (invalidExt.contains(ext)) {
                return null; 
            }
        }
        return url; 
    }
    
    /** Normalize seed URL */
    private static String normalizeSeedURL(String seed) {
        
        // step 1: handle # 
        int i = seed.indexOf('#');
        if (i != -1) {
            seed = seed.substring(0, i); // cut off after # 
        }
        
        // fill in missing port 
        String[] parts = URLParser.parseURL(seed); 
        if (parts[2] == null) {
            parts[2] = (parts[0].equals("http")) ? "80" : "443";
        }
        String url = parts[0] + "://" + parts[1] + ":" + parts[2]  + parts[3]; 

        return url; 
    }
    
    
    /** Helper for checking timestamp 
     * @param kvs 
     * @param url to be requested 
     * @return true if enough time has passed between requests 
     * @throws IOException 
     * @throws FileNotFoundException */
    private static boolean rateLimit(KVSClient kvs, String url, double delay) throws Exception {
        String[] parts = URLParser.parseURL(url); 
        String host = parts[1]; 
        
        byte[] lastAccessed = kvs.get(HOSTS, Hasher.hash(host), "lastAccessed");
        if (lastAccessed == null) {
            return true; 
        }
        
        String str = new String(lastAccessed, StandardCharsets.UTF_8); // Using UTF-8 encoding
        long lastAccessedTime = Long.valueOf(str); 
        int interval = 1 ; 
        
        if (System.currentTimeMillis() - lastAccessedTime > 1000 * delay * interval) {
            return true; 
        } 
        return false; 
    }
    
    
    /** Download robots.txt if hasn't been downloaded before 
     * Parse rules for * or "cis5550-crawler" and cache in KVS "hosts" table 
     * Parse delay for * or "cis5550-crawler" and cache in KVS "hosts" table 
     * @param kvs client to access "hosts" table 
     * @param normalized url to be requested 
     * @throws IOException 
     * @throws FileNotFoundException 
     * */
    private static void downloadRobots(KVSClient kvs, String url) throws Exception  {
        String host = URLParser.parseURL(url)[1]; 
        if (kvs.existsRow(HOSTS, Hasher.hash(host))){
            return ; 
        }
       
        String[] parts = URLParser.parseURL(url); 
        String robotsURL = parts[0] + "://" + parts[1] + ":" + parts[2]  + "/robots.txt"; 
        
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(robotsURL).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "cis5550-crawler");
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(8000);
            conn.setInstanceFollowRedirects(false);
            
            if (conn.getResponseCode() == 200) { // save robots.txt in the corresponding row 
                byte[] content = conn.getInputStream().readAllBytes();     
                String rules = robotsParser(content); 
                if (rules != null) {
                    // TODO: optimize write by writing whole rule 
                    kvs.put(HOSTS, Hasher.hash(host), "host", host);
                    kvs.put(HOSTS, Hasher.hash(host), "rules", rules); // cache rules 
                    double delay = parseDelay(rules);
                    kvs.put(HOSTS, Hasher.hash(host), "delay", String.valueOf(delay)); // cache delay
                }
            }
        } catch (Exception e) {
            kvs.put("no-crawl", Hasher.hash(host), "host", host);
        }
         
    }

    /**
     * @return return rules in one string -- could contain up to 3 lines  */
    public static String robotsParser(byte[] content) {
        Map<String, String> agents = new HashMap<>(); // agent as key and rules as values 
        
        // go through all the file. If encounter * parse and store 
        // if later encounter cis5550, overwrite 
        String str = new String(content, StandardCharsets.UTF_8); 
        Pattern pattern = Pattern.compile("user-agent: ", Pattern.CASE_INSENSITIVE);
        
        // split rules by user-agent
        String[] parts = pattern.split(str);

        for (String part: parts) {
            if (part.length() == 0) {
                continue; 
            }
            
            String[] rules = part.split("\n", 2); 
            if (rules.length < 2) {
                continue; 
            }
            agents.put(rules[0].trim(), rules[1].trim()); 
        } 
        
        if (agents.containsKey("cis5550-crawler")) {
            return agents.get("cis5550-crawler"); 
        } else if (agents.containsKey("*")) {
            return agents.get("*"); 
        }
        
        return null ; 
    }
    
    public static boolean matched(String pattern, String url) {
        int len = pattern.length(); 
        if (url.length() < len) {
            return false; 
        }
        
        if (url.substring(0, len).equals(pattern)){
            return true; 
        }
        return false; 
    }
    
    /** Check the url against allow and disallow rules 
     * @return true when excluded, ie. do not continue crawl 
     * @return false when not excluded, ie. continue crawl */
    public static boolean exclusion(String rules, String url) {
        String[] lines =  rules.split("\n");
        for (String line : lines) {
            if (line.length() == 0) {
                continue; 
            }
            String[] parts = line.split(":");
            
            // whichever of allow or disallow that matches first is directly returned 
            String link = URLParser.parseURL(url)[3];
            
            if (parts.length == 1) { // prevent exception like "Disallow:"
                return false; 
            }
            
            if (parts[0].toLowerCase().equals("allow") && matched(parts[1].trim(), link)) {
                return false; 
            } else if (parts[0].toLowerCase().equals("disallow") && matched(parts[1].trim(), link)) {
                return true; 
            }
        }
            // if neither matched, allow 
        return false; 
        
    }
    
    public static double parseDelay(String rules) {
        String[] lines = rules.split("\n"); 
        for (String line: lines) {
            if (line.length() == 0) {
                continue; 
            }
            String[] tokens = line.split(":");
            if (tokens[0].toLowerCase().equals("crawl-delay")) {
                return Double.valueOf(tokens[1].trim()); 
            }
        }
        return 1.0; 
    }
    
    /**
     * Helper for excluding URLs: 
     * @return s if ok to proceed and make requests 
     * @return "" if not enough time has passed -- enque again 
     * @return null if don't crawl */
    public static String prep(KVSClient kvs, String[] urlParts, String s) throws Exception {
        String rowKey = Hasher.hash(urlParts[1]);  // host hashed
     // if host in "no-crawl" table, skip it 
        if (kvs.existsRow("no-crawl", rowKey)) {
            return null; 
        }
        String key = Hasher.hash(s);  // url hashed 
        if (kvs.existsRow("no-crawl-url", key)) {
            return null; 
        }
        // check whether this page has been downloaded before 
        if (kvs.existsRow(CRAWLED, key)) {
            return null; 
        }
        
        // before making any request, check robot exclusion 
        downloadRobots(kvs, s); // this will put robots.txt in the "hosts" table if it's not already cached 
        byte[] content = kvs.get(HOSTS, rowKey, "rules"); 
        
        String rules = null; 
        if (content != null) {
            rules = new String(content, StandardCharsets.UTF_8); 
        }
        
        double delay = 1.0; 
        
        // parse exclusion from rules 
        if (rules != null && exclusion(rules, s)) {  // parse exclusion 
            return null;  // excluded, return empty    
        }
        
        // read delay cached in "hosts" kvs table 
        byte[] delayCached = kvs.get(HOSTS, rowKey, "delay");
        if (delayCached != null) {
            delay = Double.valueOf(new String(delayCached, StandardCharsets.UTF_8)); 
        }
        
        // not enough time has passed, wait for next round 
        if (!rateLimit(kvs, s, delay)) {
            return ""; // back to queue 
        }
        return s; // ok to make reqests 
    }
     
    
   

    /**@input  byte[] of html page content
     * @output byte[] of compressed page ready to be written to table */
    public static byte[] compress(byte[] page) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(); 
        DeflaterOutputStream dos = new DeflaterOutputStream(out);
        
        try {
            dos.write(page);
            dos.flush();   
            dos.finish(); 
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
        } 
        byte[] res = out.toByteArray();
        return res;
    }
    
    
    
}
