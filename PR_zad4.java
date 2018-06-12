import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Console;
import scala.Tuple2;

import java.util.*;

public class Main {

    private static Double M = 10.0;
    private static Double L = 1.0;

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\winutils");
        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("PR project");


        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> expresionFile = sc.textFile("src/main/resources/keywords.txt");
        String partialExpressionString = expresionFile
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator()).collect()
                .toString().replaceAll("[\\[\\]]|(, )", "");

        String specialSignsString = "[\\[\\](){}.,;:?\\-!0-9+=<>\"\'|| ]+";

        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        JavaPairRDD<String, String> textFiles = sc.wholeTextFiles("src/main/resources/testResources");

        //get file paths
//        for (String name: textFiles.keys().collect()
//             ) {
//            Console.println(name);
//            Console.println();
//        }


        for (String name : textFiles.values().collect()) {
            Console.println(name);
        }

        JavaRDD<String> partiallyCleanTexts = textFiles.values()
                .flatMap(s -> Arrays.asList(s.replaceAll("[\n\t\r]", " ").replaceAll(" +" + partialExpressionString + " +", " ")).iterator());

        JavaRDD<String> cleanTexts = partiallyCleanTexts.flatMap(s -> Arrays.asList(s.replaceAll(specialSignsString, " ")).iterator());

        List<JavaRDD<String>> separateTexts = new ArrayList<>();

        // Postać oczyszczona

        for (String name : cleanTexts.collect()) {
            separateTexts.add(sc.parallelize(Arrays.asList(name.split(" "))));
        }

        //Ilości wystąpień słówek

        List<JavaPairRDD<String, Double>> separateCounts = new ArrayList<>();
        for (JavaRDD<String> separateText : separateTexts) {
            separateCounts.add(separateText.flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                    .mapToPair(word -> new Tuple2<String, Double>(word, 1.0))
                    .reduceByKey((a, b) -> a + b));
        }

//        JavaPairRDD<String, Double> counts = cleanTexts
//                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
//                .mapToPair(word -> new Tuple2<String, Double>(word, 1.0))
//                .reduceByKey((a, b) -> a + b);

//        for (Tuple2<String, Double> name: counts.collect()) {
//            Console.println(name._1);
//            Console.println();
//        }
//
        List<Double> separateOccurrences = new ArrayList<>();

        for (JavaPairRDD<String, Double> separateCount : separateCounts) {
            separateOccurrences.add(Double.parseDouble(Long.toString(separateCount.count())));
        }

        Double totalOccurences = separateOccurrences.stream().mapToDouble(Double::doubleValue).sum();

        List<List<Tuple2<Double, String>>> separateFrequencies = new ArrayList<>();
        List<List<Tuple2<Double, String>>> globalPartialFrequencies = new ArrayList<>();
        Iterator<Double> occurIterator = separateOccurrences.iterator();

        for (JavaPairRDD<String, Double> separateCount : separateCounts) {

            List<Tuple2<Double, String>> singleLocalFrequency = new ArrayList<>();
            List<Tuple2<Double, String>> singleGlobalFrequency = new ArrayList<>();
            Double wordOccurence = occurIterator.next();

            for (Tuple2<String, Double> elem : separateCount.collect()) {
                singleLocalFrequency.add(new Tuple2<>(elem._2 / wordOccurence, elem._1));
                singleGlobalFrequency.add(new Tuple2<>(elem._2 / totalOccurences, elem._1));
            }
            separateFrequencies.add(singleLocalFrequency);
            globalPartialFrequencies.add(singleGlobalFrequency);
        }

        //Po wyliczeniu częstości lokalnych i globalnych

        List<JavaPairRDD<Double, String>> frequentWords = new ArrayList<>();

        int counter = 1;
        for (List<Tuple2<Double, String>> separateFrequency : separateFrequencies) {

            Console.println("Processing file nr: " + counter++);

            frequentWords.add(sc.parallelizePairs(separateFrequency).filter(freq -> freq._1 > L / 100).sortByKey(false));

            for (Tuple2<Double, String> frequency : separateFrequency) {
                Console.println(frequency);
            }

            Console.println();
        }

        List<JavaPairRDD<Double, String>> globallyFrequentWords = new ArrayList<>();

        for (List<Tuple2<Double, String>> globalPartialFrequency : globalPartialFrequencies) {
            globallyFrequentWords.add(sc.parallelizePairs(globalPartialFrequency));
        }

        List<Tuple2<String, Double>> globalFrequence = new ArrayList<>();

        for (JavaPairRDD<Double, String> globallyFrequentWord : globallyFrequentWords) {

            for (Tuple2<Double, String> frequentWord : globallyFrequentWord.collect()) {
                globalFrequence.add(new Tuple2<>(frequentWord._2, frequentWord._1));
            }
        }

        JavaPairRDD<Double, String> globalParallelFrequence = sc.parallelizePairs(globalFrequence)
                .reduceByKey((f, s) -> f + s)
                .filter(freq -> freq._2 > (L / 100))
                .mapToPair((tuple) -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false);

        counter = 0;
        Console.println("Global freqs: ");
        for (Tuple2<Double, String> globalPartialFrequency : globalParallelFrequence.collect()) {
            if (counter++ < M)
                Console.println(globalPartialFrequency);
            else
                break;
        }

        //Po wypisaniu listy najczęstszych słówek

        counter = 0;
        long[] frequencyCoverage = new long[separateOccurrences.size()];
        for (List<Tuple2<Double, String>> separateFrequency : separateFrequencies) {

            frequencyCoverage[counter] = sc.parallelizePairs(separateFrequency)
                    .mapToPair((tuple) -> new Tuple2<>(tuple._2, 1))
                    .intersection(
                            globalParallelFrequence.mapToPair((tuple) -> new Tuple2<>(tuple._2, 1))
                    )
                    .count();

            Console.println(frequencyCoverage[counter++]);
        }

        Iterator<Double> occurencesIter = separateOccurrences.listIterator();
        Iterator<String> fileNamesIter = textFiles.keys().collect().iterator();

        for (long l : frequencyCoverage) {
            double occurCount = l / occurencesIter.next();
            Console.println(occurCount);
            if (occurCount > 0.5)
                Console.println(fileNamesIter.next());
        }
    }
}
