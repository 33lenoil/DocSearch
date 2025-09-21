package cis5550.jobs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.*;

// javac -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar" -d classes --source-path src src/cis5550/jobs/PageRank.java
// jar cf pagerank.jar classes/cis5550/jobs/PageRank.class
// java -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar;classes" cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.01

//java -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar;classes" cis5550.kvs.Coordinator 8000
//java -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar;classes" cis5550.kvs.Worker 8001 worker1 localhost:8000
//java -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar;classes" cis5550.flame.Coordinator 9000 localhost:8000
//java -cp "lib/webserver.jar;lib/kvs.jar;lib/flame.jar;classes" cis5550.flame.Worker 9001 localhost:9000

public class PageRank {
	private static final int PREFIX_NUM = 5; 
    
    private static String prefix(String url) {
        return Hasher.hash(url).substring(0, PREFIX_NUM);
    }
    
	private static List<String> extractURL(String page){
		List<String> urls = new ArrayList<>();
		int cursor = 0, aTag;
		if (page.indexOf("<a") >= 0 && page.indexOf("<A") >= 0) {
			aTag = Math.min(page.indexOf("<a"), page.indexOf("<A"));				
		} else{
			aTag = Math.max(page.indexOf("<a"), page.indexOf("<A"));
		} 
		while (aTag >= 0) {
			// System.out.println(aTag);
			int aTagClose = page.indexOf(">", aTag + 2);
			if (aTagClose > 0) {
				String aStr = page.substring(aTag, aTagClose+1);
				int href = aStr.indexOf("href=");
				if (href > 0) {
					int urlStart = href + 6, urlEnd = aStr.indexOf("\"", urlStart);
					if (urlEnd > 0) {
						String url = aStr.substring(urlStart, urlEnd);
					// System.out.println(url);
						urls.add(url);
					}
				}
				cursor = aTagClose + 1;
			} else {
				cursor = aTag + 2;
			}
			if (page.indexOf("<a", cursor) >= 0 && page.indexOf("<A", cursor) >= 0) {
				aTag = Math.min(page.indexOf("<a", cursor), page.indexOf("<A", cursor));				
			} else{
				aTag = Math.max(page.indexOf("<a", cursor), page.indexOf("<A", cursor));
			}		
		}
		return urls;
	}
	
	private static String normalizeURL(String[] baseURL, String url){
		String normURL = null;
		String[] u = URLParser.parseURL(url);
		
		if (u[0] != null) {
			u[0] = u[0].toLowerCase();
			if (u[0].equals("http") && u[2] == null)
				u[2] = "80";
			if (u[0].equals("https") && u[2] == null)
				u[2] = "443";
			if (!u[0].equals("http") && !u[0].equals("https"))
				return null;
			int docFrag = u[3].indexOf("#");
			if (docFrag >= 0)
				u[3] = u[3].substring(0, docFrag);			
			normURL =  u[0] + "://" + u[1] + ":" + u[2] + u[3];
		}
		else {
			if (!baseURL[0].equalsIgnoreCase("http") && !baseURL[0].equalsIgnoreCase("https"))
				return null;
			normURL = baseURL[0] + "://" + baseURL[1] + ":" + baseURL[2];
			
			int docFrag = u[3].indexOf("#");
			if (docFrag >= 0)
				u[3] = u[3].substring(0, docFrag);	
			
			if (u[3].startsWith("/")) {
				normURL =  normURL + u[3];
			}
			else if (u[3].isEmpty()) {
				normURL  = normURL + baseURL[3];
			}
			else {
				String path = "";
				int basePathEnd = baseURL[3].lastIndexOf("/");
				if (basePathEnd >= 0) {
					path = baseURL[3].substring(0, basePathEnd);
				}
				String[] subpath = u[3].split("/");
				for (String p: subpath) {
					if (p.equals("..")) {
						int pathEnd = path.lastIndexOf("/");
						if (pathEnd >= 0)
							path = path.substring(0, pathEnd);
					}
					else {
						path = path + "/" + p;
					}
				}
				if (path.startsWith("/"))
					normURL = normURL + path;
				else
					normURL = normURL + "/" + path;
			}
		}
		List<String> filteredExtensions = List.of("jpg", "jpeg", "gif", "png", "txt");
		int extensionStart = normURL.lastIndexOf(".");
		if (extensionStart > 0) {
			String extension = normURL.substring(extensionStart+1);
			if (filteredExtensions.contains(extension.toLowerCase()))
				return null;
		}
		return normURL;
	}	
	
	public static void run(FlameContext ctx, String[] arg) throws Exception{
		String threshold = arg[0];
		Double percent = -1.0;
		final String kvsCoord = ctx.getKVS().getCoordinator();
		if (arg.length > 1)
			percent = Double.valueOf(arg[1]) / 100.0;
		FlamePairRDD stateData;
		if (ctx.getKVS().count("pt-state") > 0) {
			FlameRDD state = ctx.fromTable("pt-state", r -> {
				if (r.get("value") == null || r.get("value").isEmpty())
					return null;
				String data = "";
				for (String col: r.columns()) {
					if (r.get(col) != null && !r.get(col).isEmpty())
						data += r.get(col);
				}
				return r.key() + " " + r.get("value");
			});
			stateData = state.mapToPair(data -> {
				int sep = data.indexOf(" ");
				if (sep < 0)
					return null;
				String k = data.substring(0, sep);
				String v = data.substring(sep+1);
				return new FlamePair(k, v);				
			});
			state.destroy();
		}
		else {
			FlameRDD pageData = ctx.fromTable("pt-url", r -> {
				/*
				if (r.get("responseCode") == null || !r.get("responseCode").equals("200"))
					return null;
				if (r.get("page") == null || r.get("page").isEmpty())
					return null;
				*/
				if (r.get("LinkTo") == null)
					return null;
				if (r.get("url") == null || r.get("url").isEmpty())
					return null;		
				
		        return r.get("url");
			});
			FlamePairRDD pairData = pageData.mapToPair(data -> {
				KVSClient kvs = new KVSClient(kvsCoord);
				Row r = kvs.getRow("pt-url", Hasher.hash(prefix(data)+data));
				if (r == null) {
					System.out.println("Row not found");
					return null;
				}
				/*
				if (r.get("responseCode") == null || !r.get("responseCode").equals("200"))
					return null;
				if (r.get("page") == null || r.get("page").isEmpty())
					return null;
				*/
				if (r.get("LinkTo") == null)
					return null;
				if (r.get("url") == null || r.get("url").isEmpty())
					return null;
				
				/*
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
		        String p = decompressOut.toString(StandardCharsets.UTF_8);				
				List<String> urls = extractURL(p);
				String[] base = URLParser.parseURL(data);
				*/
				Set<String> set = new HashSet<>();
				if (!r.get("LinkTo").isEmpty()) {
					List<String> urls = Arrays.asList(r.get("LinkTo").split(","));
			    	for (String u: urls) {
			    		if (kvs.existsRow("pt-crawl", Hasher.hash(prefix(u)+u)))
			    			set.add(u);
			    	}
				}
		    	kvs.put("pt-urlHash", Hasher.hash(prefix(data)+data), "value", data);
				return new FlamePair(Hasher.hash(prefix(data)+data), "1.0,1.0," + String.join(";", set));
			});
			pageData.destroy();
			stateData = pairData.foldByKey("", (acc, url) -> {
				if (url != null)
					acc = url;
				return acc;
			});
			pairData.destroy();
			// stateData.saveAsTable("state");
		}
		int iter = 0;
		while (true) {
			iter++;
			FlamePairRDD transferData = stateData.flatMapToPair(state -> {
				String u = state._1();
				String[] rc_rp_L = state._2().split(",", 3);
				double rc = Double.parseDouble(rc_rp_L[0]);
				double rp = Double.parseDouble(rc_rp_L[1]);
				List<String> L = new ArrayList<>();
				if (!rc_rp_L[2].isEmpty())
					L.addAll(Arrays.asList(rc_rp_L[2].split(";")));
				double n = L.size();
				double v = rc;
				List<FlamePair> links = new ArrayList<>();
				if (n > 0 && n < 100) {
					v = 0.85 * rc / n;
					String val = Double.toString(v);
					for (String l: L)
						links.add(new FlamePair(Hasher.hash(prefix(l)+l), val));
				}
				links.add(new FlamePair(u, "0.0"));
				return links;
			});
			FlamePairRDD transfer = transferData.foldByKey("", (acc, val) -> {
				Double accNum = 0.0;
				if (acc != null && !acc.isEmpty())
					accNum = Double.parseDouble(acc);			
				if (val != null && !val.isEmpty())
					accNum += Double.parseDouble(val);
				return accNum.toString();
			});
			transferData.destroy();
			// transfer.saveAsTable("transfer");
			FlamePairRDD joinData = transfer.join(stateData);
			// FlamePairRDD joinData = stateData.join(transfer);
			transfer.destroy();
			stateData.destroy();
			FlamePairRDD nextStateData = joinData.flatMapToPair(data -> {
				String u = data._1();
				// String[] rc_rp_L_r = data._2().split(",");
				String[] r_rc_rp_L = data._2().split(",", 4);
				double rc = Double.parseDouble(r_rc_rp_L[1]);
				double r = Double.parseDouble(r_rc_rp_L[0]) + 0.15;
				String L = r_rc_rp_L[3];
				String rc_rp_L = Double.toString(r) + "," + Double.toString(rc) + "," + L;
				List<FlamePair> list = new ArrayList<>();
				list.add(new FlamePair(u, rc_rp_L));
				return list;
			});
			joinData.destroy();
			stateData = nextStateData.foldByKey("", (acc, url) -> {
				if (url != null)
					acc = url;
				return acc;
			});
			nextStateData.destroy();
			// nextState.saveAsTable("nextState");
			FlameRDD changes = stateData.flatMap(state -> {
				String[] rc_rp_L = state._2().split(",", 3);
				double rc = Double.parseDouble(rc_rp_L[0]);
				double rp = Double.parseDouble(rc_rp_L[1]);
				String change = Double.toString(Math.abs((rc - rp) / rp));
				List<String> list = new ArrayList<>();
				list.add(change);
				return list;
			});
			if (percent != -1.0) {
				FlameRDD convergedChanges = changes.filter(change -> {
					return Double.parseDouble(change) < Double.parseDouble(threshold);
				});
				// System.out.println((double)convergedChanges.count() / (double)changes.count());
				if ((double)changes.count() * percent <= (double)convergedChanges.count())
					break;
			}
			String maxChange = changes.fold("0", (c1, c2) -> "" + Math.max(Double.valueOf(c1), Double.valueOf(c2)));
			changes.destroy();
			if (Double.parseDouble(maxChange) < Double.parseDouble(threshold))
				break;
			if (iter % 3 == 0) {
				FlamePairRDD state = stateData.foldByKey("", (acc, s) -> {
					if (s != null)
						acc = s;
					return s;
				});
				ctx.getKVS().delete("pt-state");
				state.saveAsTable("pt-state");
			}
		}
		stateData.flatMapToPair(state -> {
			String u = state._1();
			String[] rc_rp_L = state._2().split(",", 3);
			String rank = rc_rp_L[0];
			KVSClient kvs = new KVSClient(kvsCoord);
			if (kvs.get("pt-urlHash", u, "value") != null) {
				String url = new String(kvs.get("pt-urlHash", u, "value"), "UTF-8");
				kvs.put("pt-pageranks", u, "rank", rank);
				kvs.put("pt-pageranks", u, "url", url);
			}
			return new ArrayList<FlamePair>();
		});
	}
}