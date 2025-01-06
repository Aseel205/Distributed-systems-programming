package org.example.APP;

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

public class googleApp_withCombiner {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;
    public static String bucketForJars = "jars123123123";
    public static int numberOfInstances = 5;

    // inputs


    private static String step1_2Input = "s3://jars123123123/step1_input.txt" ;
    private static String step3Input = "s3://jars123123123/step3_input.txt";

    private static String googleBooks3Gram_link= "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    private static String googleBooks1Gram_link= "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
    private static String step4Input_1 ="s3://jars123123123/step1_output.txt/" ;
    private static String step4Input_2 = "s3://jars123123123/step2_output.txt/" ;
    private static String step5Input = "s3://jars123123123/step4_output.txt/" ;

    // outputs
    private static String step1_output = "s3://jars123123123/step1_output.txt" ;
    private static String step2_output = "s3://jars123123123/step2_output.txt";
    private static String step3_output = "s3://jars123123123/step3_output.txt" ;
    private static String step4_output = "s3://jars123123123/step4_output.txt" ;
    private static String step5_output = "s3://jars123123123/step5_output.txt" ;

    //jars
    private static String step1_jar =  "s3://jars123123123/google jars with combiner/step1.jar" ;
    private static String step2_jar =  "s3://jars123123123/google jars with combiner/step2.jar";
    private static String step3_jar =  "s3://jars123123123/google jars with combiner/step3.jar" ;
    private static String step4_jar =  "s3://jars123123123/google jars with combiner/step4.jar" ;
    private static String step5_jar =  "s3://jars123123123/google jars with combiner/step5.jar" ;


    


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


        // Step 1
        StepConfig stepConfig1 = createStepConfig("Step1", step1_jar, googleBooks3Gram_link, " ", step1_output, "Step1" );

        // Step 2
        StepConfig stepConfig2 = createStepConfig("Step2", step2_jar, googleBooks3Gram_link, " ", step2_output, "Step2");

        // Step 3
        StepConfig stepConfig3 = createStepConfig("Step3", step3_jar, googleBooks1Gram_link, " ", step3_output, "Step3");

        // step 4
        StepConfig stepConfig4 = createStepConfig("Step4", step4_jar, step4Input_1,step4Input_2, step4_output, "Step4");

        StepConfig stepConfig5 = createStepConfig("Step5", step5_jar, step5Input," ", step5_output, "Step5");


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
                .withName( "googleApp_withCombiner")
                .withInstances(instances)
                //  .withSteps(stepConfig1)
                //     .withSteps (stepConfig2)
                //     .withSteps(stepConfig3)
                //    .withSteps(stepConfig4)

                .withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4, stepConfig5)

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

    private static StepConfig createStepConfig(String stepName, String jarPath, String inputPath, String inputPath2 ,  String outputPath, String mainClass) {


        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withArgs(inputPath, inputPath2, outputPath)
                .withMainClass(mainClass);

        return new StepConfig()
                .withName(stepName)
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
    }



}
