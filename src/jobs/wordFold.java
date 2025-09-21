package cis5550.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.*;

public class wordFold {
	public static void run(FlameContext ctx, String[] arg) throws Exception{
		final String kvsCoord = ctx.getKVS().getCoordinator();
		FlameRDD accumulator = ctx.fromTable("pt-other", r -> {
			KVSClient kvs = new KVSClient(kvsCoord); 
			List<String> acc = new ArrayList<>();
			if (r.get("acc") != null && !r.get("acc").isEmpty())
				acc.add(r.get("acc"));
			try {
				Row index = kvs.getRow("pt-index", r.key());
				if (index != null) {
					if (index.get("acc") != null && !index.get("acc").isEmpty())
						acc.add(index.get("acc"));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (acc.isEmpty())
				return null;
			/*
			List<String> urls = new ArrayList<>();
			for (String col: r.columns()) {
				if (r.get(col) != null && !r.get(col).isEmpty())
					urls.add(r.get(col));
			}
			if (urls.isEmpty())
				return null;
			*/
			Row row = new Row(r.key());
			row.put("acc", String.join("[SEP]", acc));
			try {
				kvs.putRow("pt-index", row);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return "";
		});			
	}
}
