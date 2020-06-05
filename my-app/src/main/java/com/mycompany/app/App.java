package com.mycompany.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Hello world!
 *
 */
public class App {
//    static <K,V extends Comparable<? super V>>
////    SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
////        SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
////                new Comparator<Map.Entry<K,V>>() {
////                    @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
////                        int res = e2.getValue().compareTo(e1.getValue());
////                        return res != 0 ? res : 1;
////                    }
////                }
////        );
////        sortedEntries.addAll(map.entrySet());
////        return sortedEntries;
////    }

    public static <K, V extends Comparable<V>> Map<K, V>
    sortByValues(final Map<K, V> map) {
        Comparator<K> valueComparator =
                new Comparator<K>() {
                    public int compare(K k1, K k2) {
                        int compare =
                                map.get(k2).compareTo(map.get(k1));
                        if (compare == 0)
                            return 1;
                        else
                            return compare;
                    }
                };

        Map<K, V> sortedByValues =
                new TreeMap<K, V>(valueComparator);
        sortedByValues.putAll(map);
        return sortedByValues;
    }

    public static void main( String[] args ) throws IOException {
        StringBuilder builder = new StringBuilder();
        String file ="sample_commits000000000000.json";

        // Read from file
        File myObj = new File(file);
        Scanner myReader = new Scanner(myObj);
        while (myReader.hasNextLine()) {
            String current = myReader.nextLine();
            builder.append(current);
        }

        // Collections to store results
        List<HashMap<String, Integer>> byYear = new ArrayList<>();
        List<TreeMap<String, Integer>> byYearTree = new ArrayList<>();

        //
        for (int i = 0; i < 20; i++) {
            byYear.add(new HashMap<String, Integer>());
            byYearTree.add(new TreeMap<String, Integer>());
        }
        String[] commits = builder.toString().split("\"commit\"");
        for (String commit: commits) {
            String dateMarker = "date\":";
            int dateIndex = commit.indexOf(dateMarker);
            if (dateIndex < 0) continue;
            commit = commit.substring(dateIndex + dateMarker.length() + 1);
            String year = commit.substring(0,commit.indexOf("-"));
            int index = Integer.parseInt(year) - 2000;

            if (commit.indexOf("difference") < 0) continue;;

            String marker = "\"difference\":[";
            int begArray = commit.indexOf(marker) + marker.length();
            commit = commit.substring(begArray);
            int endArr = commit.indexOf("]");
            commit = commit.substring(0, endArr);
            String[] files = commit.split("},");
            for (String f: files) {
                String marker2 = "\"new_path\":\"";
                if (f.indexOf(marker2) < 0) continue;
                int b = f.indexOf(marker2) + marker2.length();
                f = f.substring(b);
                int e = f.indexOf("\"");
                if (e < 0) continue;
                f = f.substring(0, e);
                int dot = f.lastIndexOf(".");
                if (dot >= 0) {
                    String ext = f.substring(dot);
                    TreeMap<String, Integer> extensions = byYearTree.get(index);
                    extensions.put(ext, extensions.getOrDefault(ext, 0) + 1);
                }
            }

        }

        System.out.println();

        for (int i = 5; i < byYearTree.size(); i++) {
            int year = i + 2000;

            System.out.print(year + ": " );
            int j = 0;
            Map tmp = sortByValues(byYearTree.get(i));
            Set<Map.Entry<String, Integer>> entires = tmp.entrySet();

            for (Map.Entry<String, Integer> entry: entires) {
                if (j > 6) break;
                j++;
                if (j == 0)
                    System.out.println(entry);
                else
                    System.out.println("\t" + entry);
            }
            System.out.println();

        }
        System.out.println("end");
    }
}
