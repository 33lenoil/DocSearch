package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;

import java.io.IOException;
import java.util.*;

public class TfIdf {

    private static final String indexTableName = "pt-index";

    private static final String tfidfTableName = "pt-tfidf";

    private static final double a = 0.5;

    private static int totalPageCount = 1000000;

    private static double maxTfIdf = 0;

    private static KVSClient kvsClient;

    public static void run(FlameContext context, String[] args) throws Exception {

        kvsClient = context.getKVS();


        FlameRDD tfidfTable = context.fromTable(indexTableName, (row) -> {
            String word = row.key();
            String content = row.get("acc");
            if (content == null) {
                return null;
            }
            String[] urls = content.split("\\[SEP\\]");

            StringBuilder rst = new StringBuilder();

            int ni = urls.length;

            for (String urlWords : urls) {
                try {
                    String[] entries = urlWords.split(" ");
                    String url = entries[0];
                    int totalPageWordCount = Integer.parseInt(entries[1]);
                    int totalWordFreqCount = Integer.parseInt(entries[2]);
                    double tfScore = a + (1 - a) * totalWordFreqCount / totalPageWordCount;
                    double idfScore = Math.log((double) totalPageCount / (1 + ni));
                    double tfIdf = tfScore * idfScore;
//                    maxTfIdf = Math.max(maxTfIdf, tfIdf);
                    rst.append(url).append(" ").append(tfIdf).append("[SEP]");
                } catch (Exception e) {
                    System.out.println("Error processing urlWords: " + urlWords);
                }
            }

            return rst.toString();
        });


        tfidfTable.saveAsTable(tfidfTableName);


    }


}