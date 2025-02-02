package org.example.Steps;

import java.io.*;
import java.util.*;

public class WordPairSimalirityAmeer {


    private static final String WORD_RELATEDNESS_FILE = "src/main/resources/inputFiles/word-relatedness.txt";
    private static final String STEP3_OUTPUT_FOLDER = "src/main/resources/OutputFiles/step3NewOutput";

    // Method to read word-relatedness file
    public static List<String[]> readWordRelatednessFile() throws IOException {
        List<String[]> wordPairs = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(WORD_RELATEDNESS_FILE))) {
            String line;
            System.out.println("Reading word-relatedness file...");
            while ((line = reader.readLine()) != null) {
                // Split by one or more spaces
                String[] parts = line.split("\\s+");
                parts[0] =  parts[0] .toLowerCase() ;
                 parts[1] =  parts[1].toLowerCase() ;

                wordPairs.add(parts);


            }
        }
        System.out.println("Finished reading word-relatedness file.");
        return wordPairs;
    }


    // Method to read step 3 output files with debug prints
    public static Map<String, double[][]> readStep3OutputFiles() throws IOException {
        Map<String, double[][]> wordVectors = new HashMap<>();
        File folder = new File(STEP3_OUTPUT_FOLDER); // Path to the folder

        System.out.println("Reading step 3 output files from folder: " + STEP3_OUTPUT_FOLDER);

        // Ensure the folder exists and is a directory
        if (!folder.exists() || !folder.isDirectory()) {
            throw new IOException("Directory not found: " + folder.getAbsolutePath());
        }

        // Iterate through all files in the folder
        File[] files = folder.listFiles();
        if (files == null) {
            throw new IOException("Unable to list files in directory: " + folder.getAbsolutePath());
        }

        System.out.println("Found " + files.length + " files in the folder.");

        for (File file : files) {
            // Process only regular files (skip directories)
            if (file.isFile()) {
                System.out.println("Reading file: " + file.getName());
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        String word = parts[0];
                        double[][] vectors = new double[4][1000]; // 4 vectors, each with 1000 elements

                        for (int i = 0; i < 1000; i++) {
                            for (int j = 0; j < 4; j++) {
                                String[] vectorValues = parts[i+1].split(",");
                                vectors[j][i] = Double.parseDouble(vectorValues[j]);

                            }
                        }

                        wordVectors.put(word, vectors);

                    }
                }
            }
        }

        System.out.println("Finished reading step 3 output files.");
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

        // Calculate the norms (magnitudes) of the vectors
        double norm1 = 0, norm2 = 0;
        for (int i = 0; i < vec1.length; i++) {
            norm1 += Math.pow(vec1[i], 2);
            norm2 += Math.pow(vec2[i], 2);
        }
        norm1 = Math.sqrt(norm1);
        norm2 = Math.sqrt(norm2);

        // Avoid division by zero
        if (norm1 == 0 || norm2 == 0) {
            return 0.0;
        }

        // Calculate the dot product and divide by the product of the norms
        double dotProduct = 0;
        for (int i = 0; i < vec1.length; i++) {
            dotProduct += vec1[i] * vec2[i];
        }

        // Calculate cosine similarity
        double result = dotProduct / (norm1 * norm2);


        return result;
    }


    // Jaccard Similarity (Binary)

    public static double jaccardSimilarity(double[] vec1, double[] vec2) {
        double intersection = 0, union = 0;
        for (int i = 0; i < vec1.length; i++) {
            intersection += Math.min(vec1[i], vec2[i]); // Minimum values
            union += Math.max(vec1[i], vec2[i]);       // Maximum values
        }
        return union == 0 ? 0.0 : intersection / union; // Avoid division by zero
    }

    // Dice Similarity
    public static double diceSimilarity(double[] vec1, double[] vec2) {
        double intersection = 0, sum1 = 0, sum2 = 0;
        for (int i = 0; i < vec1.length; i++) {
            intersection += Math.min(vec1[i], vec2[i]); // Minimum values
            sum1 += vec1[i];                            // Sum of vec1 values
            sum2 += vec2[i];                            // Sum of vec2 values
        }
        double denominator = sum1 + sum2;
        return denominator == 0 ? 0.0 : (2 * intersection) / denominator; // Avoid division by zero
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
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("src/main/resources/OutputFiles/step4Output/similarityScoresAmeer.txt"))) {

            for (String[] pair : wordPairs) {
                String word1 = pair[0];
                String word2 = pair[1];
                String relation = pair[2]; // The relation is not used in this case, but you could log it if needed

                // Retrieve the vectors for each word
                double[][] vectors1 = wordVectors.get(word1);
                double[][] vectors2 = wordVectors.get(word2);

                // Skip the pair if either word doesn't have vectors
                if (vectors1 == null || vectors2 == null) {
                    System.out.println("Skipping pair due to missing vectors: " + word1 + ", " + word2);
                    continue;
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

                // Format the output to group scores by similarity method
                StringBuilder formattedScores = new StringBuilder();

                for (int methodIndex = 0; methodIndex < 6; methodIndex++) {
                    for (int vectorIndex = 0; vectorIndex < 4; vectorIndex++) {
                        // Append each formatted similarity score
                        formattedScores.append(String.format("%.4f", similarityScores[vectorIndex * 6 + methodIndex]));
                        if (methodIndex != 5 || vectorIndex != 3) { // Avoid adding a comma after the last element
                            formattedScores.append(", ");
                        }
                    }
                }

                // Write the relation (true/false) and the formatted similarity scores
                writer.write(relation + "\t" + formattedScores.toString());
                writer.newLine(); // Move to the next line for the next pair



            }
        }
    }





    public static void main(String[] args) {
        try {
            // Debugging the first method: reading word-relatedness file
            System.out.println("Starting to read word-relatedness file...");
            List<String[]> wordPairs = readWordRelatednessFile();

            // Debug print to show the number of word pairs read
            System.out.println("Total word pairs read: " + wordPairs.size());

            // Debugging the second method: reading step 3 output files
            System.out.println("Reading step 3 output files...");
            Map<String, double[][]> wordVectors = readStep3OutputFiles();

            // Debug print to show the size of wordVectors map
            System.out.println("Total words with vectors read: " + wordVectors.size());

            // Debugging the third method: computing similarities
            System.out.println("Computing similarities for word pairs...");
            computeSimilarities(wordPairs, wordVectors);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}