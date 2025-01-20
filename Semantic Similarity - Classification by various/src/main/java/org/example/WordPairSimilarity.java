package org.example;

import java.io.*;
import java.util.*;

public class WordPairSimilarity {


    private static final String WORD_RELATEDNESS_FILE = "src/main/resources/inputs/word-relatedness.txt";
    private static final String STEP3_OUTPUT_FILE = "src/main/resources/outputs/step3_output";

    // Method to read word-relatedness file
    public static List<String[]> readWordRelatednessFile() throws IOException {
        List<String[]> wordPairs = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(WORD_RELATEDNESS_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Split by one or more spaces
                String[] parts = line.split("\\s+");
                wordPairs.add(parts);
            }
        }
        return wordPairs;
    }

        // Method to read step3_output file
    public static Map<String, double[][]> readStep3OutputFile() throws IOException {
        Map<String, double[][]> wordVectors = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(STEP3_OUTPUT_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                String word = parts[0];
                double[][] vectors = new double[4][1000];  // 4 vectors, each with 1000 elements
                for (int i = 1; i <= 4; i++) {
                    String[] vectorValues = parts[i].split(",");
                    for (int j = 0; j < vectorValues.length; j++) {
                        vectors[i - 1][j] = Double.parseDouble(vectorValues[j]);
                    }
                }
                wordVectors.put(word, vectors);
            }
        }
        return wordVectors;
    }


////  Distance functions


    // Manhattan Distance
    public static double manhattanDistance(double[] vec1, double[] vec2) {
        double sum = 0;
        for (int i = 0; i < vec1.length; i++) {
            sum += Math.abs(vec1[i] - vec2[i]);
        }
        return sum;
    }

    // Euclidean Distance
    public static double euclideanDistance(double[] vec1, double[] vec2) {
        double sum = 0;
        for (int i = 0; i < vec1.length; i++) {
            sum += Math.pow(vec1[i] - vec2[i], 2);
        }
        return Math.sqrt(sum);
    }

    // Cosine Similarity
    public static double cosineSimilarity(double[] vec1, double[] vec2) {
        double dotProduct = 0, norm1 = 0, norm2 = 0;
        for (int i = 0; i < vec1.length; i++) {
            dotProduct += vec1[i] * vec2[i];
            norm1 += Math.pow(vec1[i], 2);
            norm2 += Math.pow(vec2[i], 2);
        }
        if (norm1 == 0 || norm2 == 0) {
            return 0.0; // Avoid division by zero
        }
        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }
    // Jaccard Similarity (Binary)
    public static double jaccardSimilarity(double[] vec1, double[] vec2) {
        double intersection = 0, union = 0;
        for (int i = 0; i < vec1.length; i++) {
            if (vec1[i] > 0 && vec2[i] > 0) {
                intersection++;
            }
            if (vec1[i] > 0 || vec2[i] > 0) {
                union++;
            }
        }
        return union == 0 ? 0.0 : intersection / union; // Avoid NaN if union is zero
    }


    // Dice Similarity
    public static double diceSimilarity(double[] vec1, double[] vec2) {
        double intersection = 0, sum = 0;
        for (int i = 0; i < vec1.length; i++) {
            if (vec1[i] > 0 && vec2[i] > 0) {
                intersection++;
            }
            if (vec1[i] > 0 || vec2[i] > 0) {
                sum++;
            }
        }
        return (2 * intersection) / sum;
    }

    // Jensen-Shannon Divergence
    public static double jensenshannonDivergence(double[] vec1, double[] vec2) {
        double[] avg = new double[vec1.length];
        for (int i = 0; i < vec1.length; i++) {
            avg[i] = (vec1[i] + vec2[i]) / 2;
        }
        return (klDivergence(vec1, avg) + klDivergence(vec2, avg)) / 2;
    }

    public static double klDivergence(double[] p, double[] q) {
        double sum = 0;
        for (int i = 0; i < p.length; i++) {
            if (p[i] > 0 && q[i] > 0) {
                sum += p[i] * Math.log(p[i] / q[i]);
            }
        }
        return sum;
    }

    public static void computeSimilarities(List<String[]> wordPairs, Map<String, double[][]> wordVectors) throws IOException {
        // Create a BufferedWriter to write to the file
        BufferedWriter writer = new BufferedWriter(new FileWriter("src/main/resources/outputs/similarityScores.txt"));

        for (String[] pair : wordPairs) {
            String word1 = pair[0];
            String word2 = pair[1];
            String relation = pair[2]; // The relation is not used in this case, but you could log it if needed

            // Retrieve the vectors for each word
            double[][] vectors1 = wordVectors.get(word1);
            double[][] vectors2 = wordVectors.get(word2);

            if (vectors1 == null || vectors2 == null) {
                continue; // Skip if any of the words don't have vectors
            }

            // Create an array to store the 24 similarity values for this pair
            double[] similarityScores = new double[24];

            // Compare the vectors using each method and store the results
            for (int i = 0; i < 4; i++) {  // Iterate over the 4 vectors
                similarityScores[i * 6] = manhattanDistance(vectors1[i], vectors2[i]);
                similarityScores[i * 6 + 1] = euclideanDistance(vectors1[i], vectors2[i]);
                similarityScores[i * 6 + 2] = cosineSimilarity(vectors1[i], vectors2[i]);
                similarityScores[i * 6 + 3] = jaccardSimilarity(vectors1[i], vectors2[i]);
                similarityScores[i * 6 + 4] = diceSimilarity(vectors1[i], vectors2[i]);
                similarityScores[i * 6 + 5] = jensenshannonDivergence(vectors1[i], vectors2[i]);
            }

            // Write the word pair and the similarity scores to the file
            writer.write(word1 + "\t" + word2 + "\t" + String.join(",", Arrays.toString(similarityScores).replaceAll("[\\[\\] ]", "")));
            writer.newLine(); // Move to the next line for the next pair
        }

        // Close the writer
        writer.close();
    }



    public static void main(String[] args) {
        try {
            List<String[]> wordPairs = readWordRelatednessFile();
            Map<String, double[][]> wordVectors = readStep3OutputFile();
            computeSimilarities(wordPairs, wordVectors);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
