package org.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Random;

public class Test {

    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;


    public static void main(String[] args) {
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to AWS");


        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();


        String wordsCount = extractNumberFromStep3("jars123123123", "step3_output.txt");

        System.out.println(wordsCount);


    }


    public static String extractNumberFromStep3(String bucketName, String outputPath) {
        try {
            S3Object object = S3.getObject(bucketName, outputPath); // Download the file
            S3ObjectInputStream inputStream = object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    // Assuming the output format is "word number"
                    String[] parts = line.split("\\s+");
                    if (parts.length == 2) {
                        String number = parts[1];
                        System.out.println("[INFO] Extracted number: " + number);
                        return number;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to parse Step 3 output: " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

}
