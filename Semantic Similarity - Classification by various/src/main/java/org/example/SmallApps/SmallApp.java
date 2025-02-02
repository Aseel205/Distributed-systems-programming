package org.example.SmallApps;

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

import java.util.ArrayList;
import java.util.List;

public class SmallApp {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 4;

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


        // List of file numbers (5, 15, 25, ..., 95)
        int[] fileNumbers = {15, 25, 35, 45, 55, 65, 75, 85, 95};

        List<StepConfig> steps = new ArrayList<>();

            String inputPath1 = "s3://biarcs/"+fileNumbers[0]+".txt"; // Specific file
            String inputPath2 = "s3://biarcs/"+fileNumbers[1]+".txt"; // Specific file
            String inputPath3 = "s3://biarcs/"+fileNumbers[2]+".txt"; // Specific file
            String inputPath4 = "s3://biarcs/"+fileNumbers[3]+".txt"; // Specific file
            String inputPath5 = "s3://biarcs/"+fileNumbers[4]+".txt"; // Specific file
            String inputPath6 = "s3://biarcs/"+fileNumbers[5]+".txt"; // Specific file
            String inputPath7 = "s3://biarcs/"+fileNumbers[6]+".txt"; // Specific file
            String inputPath8 = "s3://biarcs/"+fileNumbers[7]+".txt"; // Specific file
            String inputPath9 = "s3://biarcs/"+fileNumbers[8]+".txt"; // Specific file
            String outputPath = "s3://aseelhamzahw3/smallOuputs/combined1"; // Unique output path for each file

            HadoopJarStepConfig step = new HadoopJarStepConfig()
                    .withJar("s3://aseelhamzahw3/jars/step1.jar")
                    .withMainClass("Step1")
                    .withArgs(inputPath1,  inputPath2 ,  inputPath3 , inputPath4, inputPath5,inputPath6,inputPath7,inputPath8,inputPath9, outputPath); // Single file as input, unique output path

            StepConfig stepConfig = new StepConfig()
                    .withName("Process File ")
                    .withHadoopJarStep(step)
                    .withActionOnFailure("TERMINATE_JOB_FLOW");

            steps.add(stepConfig);


        // Job flow configuration
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Assignment 3 - step1 SmallInput  with combiner")
                .withInstances(instances)
                .withSteps(steps)
                .withLogUri("s3://aseelhamzahw3/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
