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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

public class littleApp {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 1;

    public static void main(String[]args){
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to aws");
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
        System.out.println( "list cluster");
        //   System.out.println( emr.listClusters());
        System.out.println( " no need to print the list ");

        String wordsCount = extractNumberFromStep3("jars123123123", "step3_output.txt");
        System.out.println("the word count is : " + wordsCount);
        String inputStep4 = "s3://jars123123123/step4_input.txt" ;
        String outputStep4 = "s3://jars123123123/step4_output.txt";
        System.out.println("the word count : " +  wordsCount);
        StepConfig stepConfig4 = createStepConfig("Step4", "s3://jars123123123/step4.jar", inputStep4, outputStep4, "Step4",wordsCount);

        // job flow configuration
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
                .withSteps(stepConfig4)
                .withLogUri("s3://aseellogs")
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



    private static StepConfig createStepConfig(String stepName, String jarPath, String inputPath, String outputPath, String mainClass, String  extraArg) {
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withArgs(inputPath, outputPath, extraArg)
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
