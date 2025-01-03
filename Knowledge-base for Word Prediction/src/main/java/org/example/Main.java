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

public class Main {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;
    public static String bucketForJars = "jars123123123";
    public static int numberOfInstances = 6;
    private static String googleBooks_link= "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data";


    public static void main(String[] args) {
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to AWS");

        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        Random rand = new Random();

        // Step 1
        String inputStep1 = googleBooks_link;
        String outputStep1 = "s3://jars123123123/outputs/step1_step2_output/" + rand.nextInt(1000);
        StepConfig stepConfig1 = createStepConfig("Step1", "s3://" + bucketForJars + "/jars/Step1.jar", inputStep1, outputStep1, "Step1");

        // Step 2
        String inputStep2 = googleBooks_link;
        String outputStep2 = "s3://jars123123123/outputs/step1_step2_output/" + rand.nextInt(1000);
        StepConfig stepConfig2 = createStepConfig("Step2", "s3://" + bucketForJars + "/jars/Step2.jar", inputStep2, outputStep2, "Step2");

        // Step 3
        String inputStep3 = googleBooks_link;
        String outputStep3 = "s3://jars123123123/outputs/step3_output/" + rand.nextInt(1000);
        StepConfig stepConfig3 = createStepConfig("Step3", "s3://" + bucketForJars + "/jars/Step3.jar", inputStep3, outputStep3, "Step3");

        String wordsCount = extractNumberFromStep3("jars123123123", "outputs/step3_output/some_file.txt");


        // Step 4
        String inputStep4 = "s3://jars123123123/outputs/step1_step2_output/" ;
        String outputStep4 = "s3://jars123123123/outputs/step4_output/" + rand.nextInt(1000);
        StepConfig stepConfig4 = createStepConfig("Step4", "s3://" + bucketForJars + "/jars/Step4.jar", inputStep4, outputStep4, "Step4",wordsCount);

        // Job flow configuration
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("[INFO] Setting up job flow");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Extract Collations - 100% Of Eng-All")
                .withInstances(instances)
                .withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4)
                .withLogUri("s3://" + bucketForJars + "/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        try {
            RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
            String jobFlowId = runJobFlowResult.getJobFlowId();
            System.out.println("[INFO] Ran job flow with ID: " + jobFlowId);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to run job flow: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static StepConfig createStepConfig(String stepName, String jarPath, String inputPath, String outputPath, String mainClass, String... extraArgs) {
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withArgs(inputPath, outputPath, Arrays.toString(extraArgs))
                .withMainClass(mainClass);

        return new StepConfig()
                .withName(stepName)
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
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

