package cis5550.jobs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.*;
import java.util.*;
import java.util.zip.*;

import cis5550.external.PorterStemmer;
import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.*;

// javac -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar" -d classes --source-path src src/cis5550/jobs/Indexer.java
// jar cf indexer.jar classes/cis5550/jobs/Indexer.class
// java -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar;classes" cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer

// java -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar;classes" cis5550.kvs.Coordinator 8000
// java -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar;classes" cis5550.kvs.Worker 8001 worker1 localhost:8000
// java -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar;classes" cis5550.flame.Coordinator 9000 localhost:8000
// java -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar;classes" cis5550.flame.Worker 9001 localhost:9000

public class Indexer {
	private static final int PREFIX_NUM = 5; 
    
    private static String prefix(String url) {
        return Hasher.hash(url).substring(0, PREFIX_NUM);
    }	
	
	public static void run(FlameContext ctx, String[] arg) throws Exception{
		final String kvsCoord = ctx.getKVS().getCoordinator();
		ctx.getKVS().delete("pt-index");
		/*
		Iterator<Row> iter = ctx.getKVS().scan("pt-crawl");
		int count = 0;
		String fromKey = null;
		while (true) {
			while (iter.hasNext() && count < 100) {
				Row r = iter.next();
				ctx.getKVS().putRow("crawl", r);
				count++;
			}
			count = 0;
			if (iter.hasNext())
				fromKey = iter.next().key();
		*/
			FlameRDD pageData = ctx.fromTable("pt-crawl", r -> {
				/*
				if (r.get("responseCode") == null || !r.get("responseCode").equals("200"))
					return null;
				*/
				if (r.get("page") == null || r.get("page").isEmpty() || r.get("page").equalsIgnoreCase("null")) {
					// System.out.println("Invalid page");
					return null;					
				}
				if (r.get("url") == null || r.get("url").isEmpty()) {
					// System.out.println("Invalid url");
					return null;
				}
				return r.get("url");
			});
			// ctx.getKVS().delete("crawl");
			FlamePairRDD pairData = pageData.mapToPair(data -> {		
				// kvs.put("urlHash", Hasher.hash(u), "value", u);
				return new FlamePair(Hasher.hash(prefix(data)+data), data);
			});
			pageData.destroy();
			FlamePairRDD indexData = pairData.flatMapToPair(pair -> {
				// String hashedURL = pair._1(), page = pair._2();
				String url = pair._2();
				KVSClient kvs = new KVSClient(kvsCoord);
				Row r = kvs.getRow("pt-crawl", pair._1());
				if (r == null) {
					System.out.println("Row not found");
					return null;
				}
				/*
				if (r.get("responseCode") == null || !r.get("responseCode").equals("200"))
					return null;
				if (r.get("url") == null || r.get("url").isEmpty())
					return null;					
				*/
				if (r.get("page") == null || r.get("page").isEmpty()) {
					System.out.println("Page not found");
					return null;
				}
				// decompress
		        Inflater inf = new Inflater(); 
		        inf.setInput(r.getBytes("page"));	        
		        ByteArrayOutputStream decompressOut = new ByteArrayOutputStream(); 
		        InflaterOutputStream ios = new InflaterOutputStream(decompressOut, inf); 
		        try {
		        	ios.write(r.getBytes("page"));
		        	ios.flush();
		        } catch (IOException ie) {
		        	ie.printStackTrace();
		        }
		        String page = decompressOut.toString(StandardCharsets.UTF_8);
				Map<String, Integer> wordNum = new HashMap<>();
				/*
				String[] words = page.replaceAll("<[^>]*>", " ").replaceAll("[^a-zA-Z0-9]", " ").toLowerCase().trim().split("\\s+");
				words = Arrays.stream(words).filter(word -> word.length() <= 15 && (!word.matches("\\d+") || (word.matches("\\d+") && word.length() <= 5))).toArray(String[]::new);
				*/
		        String[] rawWords = page.toLowerCase().trim().split("\\s+");
		        String[] words = Arrays.stream(rawWords).filter(w -> !w.isEmpty() && w.matches("^[a-zA-Z0-9]+$")).toArray(String[]::new);
		        int totalWords = words.length;
				Set<FlamePair> wordUrls = new HashSet<>();
				PorterStemmer s = new PorterStemmer();
				for (int i = 0; i < words.length; i++) {
					// wordPos.computeIfAbsent(words[i], pos -> new ArrayList<String>()).add("" + i);
					wordNum.put(words[i], wordNum.getOrDefault(words[i], 0) + 1);
					s.add(words[i].toCharArray(), words[i].length());
					s.stem();
					if (!s.toString().equals(words[i]))
						wordNum.put(s.toString(), wordNum.getOrDefault(s.toString(), 0) + 1);
				}
				for (Map.Entry<String, Integer> entry: wordNum.entrySet()) {
					String word = entry.getKey();
					String url_num = url + " " + totalWords + " " + entry.getValue();
					// String url_pos = url + ":" + String.join(" ", entry.getValue());
					String regex = "^[a-zA-Z0-9]+$";
					if (!word.isEmpty() && word.matches(regex)) {					
						wordUrls.add(new FlamePair(word, url_num));
					}
				}
				
				return wordUrls;
			});
			pairData.destroy();
			FlamePairRDD index = indexData.foldByKey("", (acc, url) -> {				
				if (acc.isEmpty()) {
					return url;
				}
				else {
					/*
					acc = acc + "[SEP]" + url;
					// acc = acc + "," + url;
					List<String> url_num = Arrays.asList(acc.split("\\Q" + "[SEP]" + "\\E"));
					// List<String> url_pos = Arrays.asList(acc.split(","));
					Collections.sort(url_num, (u1, u2) -> {
						String u1_num = u1.substring((u1).lastIndexOf(" ")+1);
						String u2_num = u2.substring((u2).lastIndexOf(" ")+1);
						// String u1_pos = u1.substring((u1).lastIndexOf(":")+1);
						// String u2_pos = u2.substring((u2).lastIndexOf(":")+1);
						return Integer.compare(Integer.parseInt(u2_num), Integer.parseInt(u1_num));
					});
					return String.join("[SEP]", url_num);
					// return String.join(",", url_pos);
					*/
					return acc + "[SEP]" + url;
				}
			});
			indexData.destroy();
			/*
			List<FlamePair> list = index.collect();
			index.destroy();
			for (FlamePair p: list) {
					Row curr = ctx.getKVS().getRow("pt-index", p._1());
					if (curr == null) {
						curr = new Row(p._1());
						List<String> url_pos = Arrays.asList(p._2().split("\\Q" + "[SEP]" + "\\E"));
						Collections.sort(url_pos, (u1, u2) -> {
							String u1_pos = u1.substring((u1).indexOf(" ")+1);
							String u2_pos = u2.substring((u2).indexOf(" ")+1);
							return Integer.compare(u2_pos.split(" ").length, u1_pos.split(" ").length);
						});						
						curr.put("value", String.join("[SEP]", url_pos));
					} else {
						String acc = p._2();
						if (curr.get("value") != null && !curr.get("value").isEmpty())
							acc = acc + "[SEP]" + curr.get("value");
						List<String> url_pos = Arrays.asList(acc.split("\\Q" + "[SEP]" + "\\E"));
						Collections.sort(url_pos, (u1, u2) -> {
							String u1_pos = u1.substring((u1).indexOf(" ")+1);
							String u2_pos = u2.substring((u2).indexOf(" ")+1);
							return Integer.compare(u2_pos.split(" ").length, u1_pos.split(" ").length);
						});						
						curr.put("value", String.join("[SEP]", url_pos));						
					}
					ctx.getKVS().putRow("pt-index", curr);
			}
			// index.saveAsTable("pt-index");
			if (!iter.hasNext())
				break;
			if (fromKey == null)
				break;
			iter = ctx.getKVS().scan("pt-crawl", fromKey, null);			
		}
		*/
		index.saveAsTable("pt-index");
	}
}