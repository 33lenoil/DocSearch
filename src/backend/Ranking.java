package cis5550.backend;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.util.concurrent.ConcurrentHashMap;

import java.util.Collections;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static cis5550.tools.Hasher.hash;
import static cis5550.webserver.Server.*;

import java.util.regex.*;

public class Ranking {

    private static final String crawlTableName = "pt-crawl";
    private static final String prankTableName = "pt-pageranks";
    private static final String tfidfTableName = "pt-tfidf";
    private static KVSClient kvsClient;
    private static final Set<String> stopWords = createStopWordSet();

    // TODO: Experiment with different parameters
    private static final int defaultStart = 1, defaultEnd = 1000;
    private static final double pageRankWeight = 0.2, tfIdfWeight = 100, domainWeight = 0.05, titleWeight = 0.5;
    static double maxPageRank = 1.0;
    static double maxTfIdf = 7.5093181189268146;
    private static final int PREFIX_NUM = 5;


    static double testMaxPageRank = 0;

    private static final Map<String, Integer> domainValues = new HashMap<String, Integer>() {
        {
            put("wikipedia.org", 8);
            put("edu", 5);
            put("bbc.com", 10);
            put("cnn.com", 10);
            put("nytimes.com", 10);
        }
    };


    public static void main(String[] args) throws Exception {

        int port;
        try {
            port = Integer.parseInt(args[0]);
            kvsClient = new KVSClient(args[1]);
        } catch (Exception e) {
            System.out.println("Error Usage: java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:classes cis5550.backend.Ranking <port> <kvs>");
            return;
        }
        System.out.println("Ranking starting on port " + port + " with KVS at " + args[1]);


        port(port);
        securePort(443);

        get("/search", (request, response) -> {
            String terms = request.queryParams("terms");
            String startStr = request.queryParams("start");
            String endStr = request.queryParams("end");


            if (terms == null) {
                response.status(400, "Bad Request");
                return "Bad Request: terms are required.";
            }
            int start = defaultStart, end = defaultEnd;
            try {
                start = Integer.parseInt(startStr);
                end = Integer.parseInt(endStr);
            } catch (NumberFormatException e) {
                System.out.println("--- start/end not specified, using default value ---");
            }
            System.out.println("Ranking received request: " + terms + " " + start + " " + end);
            response.type("text/html");
            response.header("Access-Control-Allow-Origin", "*");
            long startTime = System.currentTimeMillis();
            String rst = getUrlList(terms, start, end);
            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime;
            System.out.println("Ranking Success! Running time: " + elapsedTime + " milliseconds");
            System.out.println("Max pagerank: " + testMaxPageRank);
            return rst;
        });

    }


    private static String getUrlList(String terms, int start, int end) throws IOException, URISyntaxException {

        // TODO: Step 1: reads the search terms

        terms = terms.toLowerCase();

        List<String> wordsList = preprocessWords(terms.split(":sep:"));

        Map<String, Double> urlScores = new HashMap<>();

        List<Map<String, Double>> listOfUrlScores = new ArrayList<>();

        for (String word : wordsList) {
            Row wordEntry = kvsClient.getRow(tfidfTableName, word);
            if (wordEntry == null) {
                System.out.println("No word entry found");
                continue;
            }
            String[] urls = wordEntry.get("value").split("\\[SEP\\]");
            Map<String, Double> map = Arrays.stream(urls)
                    .parallel()
                    .map(urlWords -> urlWords.split(" "))
                    .collect(HashMap::new,
                            (resultMap, entries) -> resultMap.put(entries[0], Double.parseDouble(entries[1])),
                            HashMap::putAll);
            listOfUrlScores.add(map);
        }

        Map<String, Double> sumScore = findCommonUrlsAndSum(listOfUrlScores);

        Map<String, String> urlTitles = new ConcurrentHashMap<>();
        sumScore.keySet().parallelStream().forEach(url -> {
            String hashUrl = hash(prefix(url) + url);
            Row crawlTableEntry = null;
            try {
                crawlTableEntry = kvsClient.getRow(crawlTableName, hashUrl);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String title = (crawlTableEntry != null) ? crawlTableEntry.get("title") : null;
            if (title == null) {
                title = "No Title";
            }
            urlTitles.put(url, title);
        });

        sumScore.entrySet().stream()
                .parallel() // Enable parallel processing
                .forEach(entry -> {
                    String url = entry.getKey();
                    Double tfIdf = entry.getValue();
                    double pageRank = 0, domainScore = 0, titleScore = 0;
                    try {
                        pageRank = getPageRank(url);
                        domainScore = getDomainScore(url, wordsList);
                        titleScore = getTitleScore(urlTitles.get(url), wordsList);
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    double score = pageRankWeight * pageRank / maxPageRank
                            + tfIdfWeight * tfIdf / maxTfIdf
                            + domainWeight * domainScore
                            + titleWeight * titleScore;

                    synchronized (urlScores) {
                        urlScores.put(url, score);
                    }
                });


        // TODO: Step 6: sorts the URLs by the combined scores

        List<Map.Entry<String, Double>> list = new ArrayList<>(urlScores.entrySet());
        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));


        // TODO: Step 7: assembles a “results” page, which is sent back to the user’s browser.
        if (list.size() == 0) {
            return "EMPTY";
        }

        int endPos = Math.min(list.size(), end);
        List<Map.Entry<String, Double>> subUrls = list.subList(start - 1, endPos - 1);
        List<String> resultUrls = new ArrayList<>();
        for (Map.Entry<String, Double> entry : subUrls) {
            String url = entry.getKey();
            String title = urlTitles.get(url);
            url += ":title:" + title;
            resultUrls.add(url);
        }
        return String.join(":sep:", resultUrls);
    }

    private static Map<String, Double> findCommonUrlsAndSum(List<Map<String, Double>> listOfMaps) {
        if (listOfMaps.isEmpty()) {
            return new HashMap<>();
        }

        Map<String, Double> result = new HashMap<>(listOfMaps.remove(0));

        for (Map<String, Double> map : listOfMaps) {
            Set<String> commonKeys = result.keySet();
            commonKeys.retainAll(map.keySet());

            for (String key : commonKeys) {
                result.put(key, result.get(key) + map.get(key));
            }
        }

        return result;
    }

    private static List<String> preprocessWords(String[] words) {
        List<String> result = new ArrayList<>();
        for (String word : words) {
            if (stopWords.contains(word)) {
                continue;
            }
            result.add(word);
        }
        return result;
    }

    private static double getPageRank(String url) throws IOException {
        Row row = kvsClient.getRow(prankTableName, hash(prefix(url) + url));
        if (row == null) {
            return 0;
        }

        double pageRank = Double.parseDouble(row.get("rank"));
        testMaxPageRank = Math.max(testMaxPageRank, pageRank);
        return pageRank;
    }

    private static double getDomainScore(String url, List<String> wordsList) throws URISyntaxException {
        for (String domain : domainValues.keySet()) {
            if (url.contains(domain)) {
                int score = domainValues.get(domain);
                int count = 0;
                if (domain.equals("wikipedia.org")) {
                    for (String word : wordsList) {
                        if (url.toLowerCase().contains(word)) {
                            count += 1;
                        }
                    }
                    if (count == wordsList.size()) {
                        count += 10;
                    }
                }
                return score + count * 2;
            }
        }
        return 0;
    }

    private static double getTitleScore(String title, List<String> wordsList) {
        int count = 0;
        for (String word : wordsList) {
            if (title.toLowerCase().contains(word)) {
                count += 1;
            }
        }
        if (count == wordsList.size()) {
            count += 10;
        }
        return count;
    }

    private static String prefix(String url) {
        return hash(url).substring(0, PREFIX_NUM);
    }

    private static Set<String> createStopWordSet() {
        return new HashSet<>(Set.of(
                "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't",
                "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
                "can", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing",
                "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
                "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself",
                "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is",
                "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no",
                "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves",
                "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so",
                "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
                "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to",
                "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
                "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom",
                "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your",
                "yours", "yourself", "yourselves"
                // Add more stop words as needed
        ));
    }
}