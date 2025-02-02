package org.example.BigAPPS;

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

public class App {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 7;

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

        // Single Step for all files
        String inputPath = "s3://biarcs/"; // Directory containing all 97 files
        String outputPath = "s3://aseelhamzahw3/outputs/combined"; // Combined output path

        HadoopJarStepConfig step = new HadoopJarStepConfig()
                .withJar("s3://aseelhamzahw3/jars/step1.jar")
                .withMainClass("Step1")
                .withArgs(inputPath, outputPath); // Directory as input, single output path

        StepConfig stepConfig = new StepConfig()
                .withName("Process All Files")
                .withHadoopJarStep(step)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        List<StepConfig> steps = new ArrayList<>();
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
                .withName("Assignment 3 - step1 with combiner")
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


/*

   // Check if this word is a lexeme (you can have a separate method to check for lexemes)
                                if (Lexems.contains(wordAtHeadIndex) && FeatureTable.containsKey(stemmedFeatureWord + "-" + depLabel)) {
                                    int featureIndex = featureToIndex(stemmedFeatureWord + "-" + depLabel); // Fetch index from FeatureTable

                                    // Emit <lexeme, h(feature)>, count
                                    context.write(new Text(wordAtHeadIndex), new Text(featureIndex + "," + totalCount));
                                }
                            }




 */