package org.example;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.FileOutputStream;

import java.util.HashMap;


import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class AWS {
    public final String AMI_ID = "ami-0829e00d96164fbc9";
    public final Region region1 = Region.US_EAST_1;
    protected final S3Client s3;
    protected final SqsClient sqs;
    protected final Ec2Client ec2;
    protected final String bucketName;
    protected static AWS instance = null;

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region1).build();
        bucketName = "my-bucket";
    }

    public static AWS getInstance() {
        if (instance == null) {
            return new AWS();
        }

        return instance;
    }


//////////////////////////////////////////  EC2

    //designed to launch Amazon EC2 instances using a specified Amazon Machine Image (AMI).


    //The getAllInstances function is designed to retrieve a list of all EC2 instances in the AWS account.
    public List<Instance> getAllInstancesWithLabel(Label label) throws InterruptedException {
        DescribeInstancesRequest describeInstancesRequest =
                DescribeInstancesRequest.builder()
                        .filters(Filter.builder()
                                .name("tag:Label")
                                .values(label.toString())
                                .build())
                        .build();

        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    // to kill the manger
    public void terminateInstance(String instanceId) {
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        // Terminate the instance
        ec2.terminateInstances(terminateRequest);

        System.out.println("Terminated instance: " + instanceId);
    }


    ////////////////////////////// S3

    // keyPath: A string representing the path (or key) where the file will be stored in the S3 bucket.
    //file: A File object representing the file to be uploaded.
    // The function returns a string representing the S3 URL of the uploaded file in the format:


    public void downloadFileFromS3(String keyPath, File outputFile) {
        System.out.println("Start downloading file " + keyPath + " to " + outputFile.getPath());

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            byte[] data = objectBytes.asByteArray();

            // Write the data to a local file.
            OutputStream os = new FileOutputStream(outputFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // bucket : container or storage unit used to  hold or store things
    public void createBucket(String bucketName) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build());
        s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                .bucket(bucketName)
                .build());
    }

    // The listObjectsInBucket function is designed to list the objects in an Amazon S3 bucket
    // and print information about each object (its key and size).


    public void deleteBucket(String bucketName) {
        // First delete all objects from the bucket, then try to delete the empty bucket
        deleteAllObjectsFromBucket(bucketName);
        deleteEmptyBucket(bucketName);
    }

    public void deleteEmptyBucket(String bucketName) {
        // Try deleting the empty bucket after all objects have been deleted
        try {
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3.deleteBucket(deleteBucketRequest);
            System.out.println("Bucket " + bucketName + " deleted successfully.");
        } catch (S3Exception e) {
            System.err.println("Error deleting bucket: " + e.getMessage());
            // Handle additional error handling or retries if necessary
        }
    }

    public void deleteAllObjectsFromBucket(String bucketName) {
        // List all objects (including versions if versioning is enabled)
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();
        ListObjectsV2Response listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);

        // Ensure there's at least one object to delete
        if (listObjectsV2Response.contents().isEmpty()) {
            System.out.println("No objects found in bucket " + bucketName + ". No deletion needed.");
            return;
        }

        // Collect all object keys (consider versioned objects if necessary)
        List<ObjectIdentifier> keys = listObjectsV2Response.contents().stream()
                .map(content -> ObjectIdentifier.builder()
                        .key(content.key())
                        .build())
                .collect(Collectors.toList());

        // Delete all objects in batches (limit to 1000 objects per request)
        int batchSize = 1000;
        for (int i = 0; i < keys.size(); i += batchSize) {
            List<ObjectIdentifier> batchKeys = keys.subList(i, Math.min(i + batchSize, keys.size()));
            Delete del = Delete.builder().objects(batchKeys).build();
            DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(del)
                    .build();

            try {
                s3.deleteObjects(multiObjectDeleteRequest);
                System.out.println("Batch deletion successful for bucket " + bucketName);
            } catch (S3Exception e) {
                System.err.println("Error deleting objects in bucket: " + e.getMessage());
                // Handle failure (e.g., retry or log detailed error)
            }
        }
    }


    //////////////////////////////////////////////SQS

    /**
     * @param queueName
     * @return queueUrl
     */
    public String createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        CreateQueueResponse create_result = null;
        create_result = sqs.createQueue(request);

        assert create_result != null;
        String queueUrl = create_result.queueUrl();
        System.out.println("Created queue '" + queueName + "', queue URL: " + queueUrl);
        return queueUrl;
    }

    public void deleteQueue(String queueUrl) {
        DeleteQueueRequest req =
                DeleteQueueRequest.builder()
                        .queueUrl(queueUrl)
                        .build();

        sqs.deleteQueue(req);
    }

    // the Url is the address
    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = null;
        queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        System.out.println("Queue URL: " + queueUrl);
        return queueUrl;
    }

    public int getQueueSize(String queueUrl) {
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED
                )
                .build();

        GetQueueAttributesResponse queueAttributesResponse = null;
        queueAttributesResponse = sqs.getQueueAttributes(getQueueAttributesRequest);
        Map<QueueAttributeName, String> attributes = queueAttributesResponse.attributes();

        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED));
    }


    ///////////////////////

    public enum Label {
        Manager,
        Worker
    }

    ////////////////////////// Hamza and Aseel added this  :


    public void tagInstance(String instanceId, String key, String value) {
        // Create a tag request
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(Tag.builder()
                        .key(key)
                        .value(value)
                        .build())
                .build();

        // Apply the tag to the instance
        ec2.createTags(tagRequest);

        System.out.println("Added tag to instance: " + instanceId + " - " + key + ": " + value);
    }

    public void sendMsg(String queueUrl, String msg) {

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(msg)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);

    }

    public List<Message> receiveMsg(String queueUrl) {
        // receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(5)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        return messages;
    }


    public boolean isQueueExist(String queueName) {
        try {
            // Try to get the queue URL by its name
            String queueUrl = getQueueUrl(queueName);
            // If no exception is thrown, the queue exists
            return true;
        } catch (QueueDoesNotExistException e) {
            // If a QueueDoesNotExistException is thrown, the queue does not exist
            return false;
        }
    }


//////////////////////////////////////////  S3 Bucket URL Handling

    // Function to return the URL of a bucket based on its name
    public String getBucketUrl(String bucketName) {
        // AWS S3 bucket URLs typically follow this format:
        return "https://" + bucketName + ".s3." + region1.id() + ".amazonaws.com";
    }

    // Updated createBucket function to return the bucket URL
    public String createBucketAndGetUrl(String bucketName) {
        // Create the bucket
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build());

        // Wait until the bucket exists
        s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                .bucket(bucketName)
                .build());

        // Return the URL of the created bucket
        return getBucketUrl(bucketName);
    }




    public int countInstancesWithLabel(Label label) throws InterruptedException {
        // Retrieve all instances with the specified label
        List<Instance> instancesWithLabel = getAllInstancesWithLabel(label);

        // If there are no instances to delete, print a message and return 0
        if (instancesWithLabel.isEmpty()) {
            System.out.println("No instances found with label: " + label);
            return 0;
        }

        // Return the number of instances found
        return instancesWithLabel.size();
    }



    public String uploadFileToS3(String filePath, String bucketName) throws IOException {
        // Extract the file name from the file path
        File file = new File(filePath);
        if (!file.exists()) {
            throw new FileNotFoundException("File not found: " + filePath);
        }
        String fileName = file.getName();

        // Build the PutObjectRequest
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName) // Use the file name as the object key
                .build();

        // Upload the file to S3
        s3.putObject(putObjectRequest, RequestBody.fromFile(file));

        // Return the S3 address of the uploaded file
        return "s3://" + bucketName + "/" + fileName;
    }


    public void downloadAndAddToHashtable(String fileUrl, String id, HashMap<String, File> fileHashtable) throws Exception {
        // Validate the S3 URL
        if (!fileUrl.startsWith("s3://")) {
            throw new IllegalArgumentException("Invalid S3 URL format. Expected format: s3://<bucket-name>/<object-key>");
        }

        // Extract bucket name and object key
        String[] s3Parts = fileUrl.replace("s3://", "").split("/", 2);
        if (s3Parts.length < 2) {
            throw new IllegalArgumentException("Invalid S3 URL format. Must include both bucket name and object key.");
        }
        String bucketName = s3Parts[0];
        String objectKey = s3Parts[1];

        // Create a local file path for the downloaded file
        Path localFilePath = Paths.get(System.getProperty("java.io.tmpdir"), id + "_" + objectKey);

        // Download the file and save it locally
        downloadFile(bucketName, objectKey, localFilePath);

        // Add the file to the hashtable
        fileHashtable.put(id, localFilePath.toFile());
    }

    private void downloadFile(String bucketName, String objectKey, Path localFilePath) throws IOException {
        // Build the S3 GetObjectRequest
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        // Get the S3 object as bytes
        ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);

        // Write the bytes to the local file
        try (FileOutputStream outputStream = new FileOutputStream(localFilePath.toFile())) {
            outputStream.write(objectBytes.asByteArray());
        }

        System.out.println("File downloaded successfully to: " + localFilePath.toString());
    }

    public void runInstanceFromAMI(String ami, int num, Label instanceLabel) {
        try {
            // Read AWS credentials from ~/.aws/credentials
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
            List<String> credentialsLines = Files.readAllLines(credentialsPath);

            // Extract access key, secret key, and session token
            String awsAccessKeyId = null;
            String awsSecretAccessKey = null;
            String awsSessionToken = null;

            for (String line : credentialsLines) {
                if (line.startsWith("aws_access_key_id")) {
                    awsAccessKeyId = line.split("=")[1].trim();
                } else if (line.startsWith("aws_secret_access_key")) {
                    awsSecretAccessKey = line.split("=")[1].trim();
                } else if (line.startsWith("aws_session_token")) {
                    awsSessionToken = line.split("=")[1].trim();
                }
            }

            if (awsAccessKeyId == null || awsSecretAccessKey == null || awsSessionToken == null) {
                throw new RuntimeException("Incomplete credentials in ~/.aws/credentials");
            }

            String userDataScript = generateUserDataScript(awsAccessKeyId, awsSecretAccessKey, awsSessionToken);

            // Add the label as a tag
            Tag labelTag = Tag.builder()
                    .key("Label")
                    .value(instanceLabel.name())
                    .build();

            RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                    .imageId(ami)
                    .instanceType(InstanceType.T2_MICRO)
                    .minCount(num)
                    .maxCount(num)
                    .userData(Base64.getEncoder().encodeToString(userDataScript.getBytes()))
                    .tagSpecifications(TagSpecification.builder()
                            .resourceType(ResourceType.INSTANCE)
                            .tags(labelTag)
                            .build())
                    .build();

            // Launch the instance
            ec2.runInstances(runInstancesRequest);
            System.out.println("Launched " + num + " instances with label " + instanceLabel.name());
        } catch (IOException e) {
            System.err.println("Error reading credentials file: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error launching EC2 instance: " + e.getMessage());
        }
    }


    private static String generateUserDataScript(String accessKeyId, String secretAccessKey, String sessionToken) {
        return "#!/bin/bash\n" +
                "mkdir -p ~/.aws\n" +
                "cat <<EOL > ~/.aws/credentials\n" +
                "[default]\n" +
                "aws_access_key_id=" + accessKeyId + "\n" +
                "aws_secret_access_key=" + secretAccessKey + "\n" +
                "aws_session_token=" + sessionToken + "\n" +
                "EOL\n" +
                "chmod 600 ~/.aws/credentials\n" +
                "cd ~\n" +
                "aws s3 cp s3://aseeljarfiles/Hmza_APP-1.0-SNAPSHOT.zip .\n" +
                "unzip -P Aseel Hmza_APP-1.0-SNAPSHOT.zip\n" + // Extract the ZIP with the password
                "java -jar Hmza_APP-1.0-SNAPSHOT.jar\n"; // Run the JAR file
    }


    public void deleteInstancesWithLabel(int numberOfInstancesToDelete, String amiId, Label instanceLabel) {
        try {
            // Describe all running instances
            DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(
                    DescribeInstancesRequest.builder().build()
            );

            List<Instance> runningInstances = describeInstancesResponse.reservations().stream()
                    .flatMap(reservation -> reservation.instances().stream())
                    .filter(instance -> instance.state().name() == InstanceStateName.RUNNING) // Only running instances
                    .filter(instance -> instance.imageId().equals(amiId)) // Instances from the specified AMI
                    .filter(instance -> {
                        // Check instance tag for the label
                        return instance.tags().stream()
                                .anyMatch(tag -> tag.key().equals("Label") && tag.value().equals(instanceLabel.name()));
                    })
                    .collect(Collectors.toList());

            // Limit the instances to delete based on the input parameter
            List<String> instanceIdsToDelete = runningInstances.stream()
                    .limit(numberOfInstancesToDelete)
                    .map(Instance::instanceId)
                    .collect(Collectors.toList());

            // Terminate the selected instances
            if (!instanceIdsToDelete.isEmpty()) {
                TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                        .instanceIds(instanceIdsToDelete)
                        .build();

                ec2.terminateInstances(terminateRequest);
                System.out.println("Deleted instances labeled as " + instanceLabel + " from AMI " + amiId + ": " + instanceIdsToDelete);
            } else {
                System.out.println("No instances to delete or insufficient instances available for AMI " + amiId + " with label " + instanceLabel);
            }
        } catch (Ec2Exception e) {
            System.err.println("Failed to delete instances: " + e.awsErrorDetails().errorMessage());
        }
    }



    public void deleteMessageForever(Message message, String queueUrl) {

        // If there are any messages, delete them after receiving

        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest); // Delete the message from the queue permanently
        System.out.println("the message was successfully deleted  ...");

        // Return the list of messages
    }

    public int countRunningInstancesFromAmi(String ami) {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder()
                                .name("image-id")
                                .values(ami)
                                .build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("running")
                                .build()
                )
                .build();

        DescribeInstancesResponse response = ec2.describeInstances(request);

        // Count the instances
        return response.reservations().stream()
                .flatMap(reservation -> reservation.instances().stream())
                .filter(instance -> ami.equals(instance.imageId()))
                .mapToInt(instance -> 1) // Map each matching instance to 1
                .sum(); // Sum up the instances
    }






}