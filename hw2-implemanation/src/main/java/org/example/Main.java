package org.example;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

import java.io.IOException;
// this is the APP.java they explained in the video
public class Main {

    public static void main(String[] args) {
        // this is what the EMR do
        // and this file creates a new EMR
        try {
            // Load AWS credentials from the properties file
            AWSCredentials credentials = new PropertiesCredentials(
                    Main.class.getResourceAsStream("/AwsCredentials.properties")
            );

            // Create an AmazonElasticMapReduce client
            AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                    .standard()
                    .withCredentials(() -> credentials)
                    .withRegion("us-east-1") // Change the region if necessary
                    .build();

            // Configure the Hadoop job
            HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                    .withJar("s3://yourbucket/yourfile.jar") // Use "s3://" instead of "s3n://"
                    .withMainClass("some.pack.MainClass")
                    .withArgs("s3://yourbucket/input/", "s3://yourbucket/output/");

            StepConfig stepConfig = new StepConfig()
                    .withName("stepname")
                    .withHadoopJarStep(hadoopJarStep)
                    .withActionOnFailure("TERMINATE_JOB_FLOW");
              .withMasterInstanceType("m4.large")
                    .withSlaveInstanceType("m4.large")
                    .withKeepJobFlowAliveWhenNoSteps(false)
                    .withPlacement(new PlacementType().withAvailabilityZone("us-east-1a"));

            // Create and submit the job flow request
            RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                    .withName("jobname")
                    .withInstances(instances)
                    .withSteps(stepConfig)
                    .withLogUri("s3://yourbucket/logs/");

            RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);

            // Output the job flow ID
            String jobFlowId = runJobFlowResult.getJobFlowId();
            System.out.println("Ran job flow with ID: " + jobFlowId);

        } catch (IOException e) {
            System.err.println("Error loading AWS credentials: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error running job flow: " + e.getMessage());
        }
    }
            // Configure job flow instances
            JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                    .withInstanceCount(2) ;


/* reading the n-grams File
    Configuration conf = new Configuration();
    Job job = new Job(conf, "...");
...
        job.setInputFormatClass(SequenceFileInputFormat.class);
*/
}


