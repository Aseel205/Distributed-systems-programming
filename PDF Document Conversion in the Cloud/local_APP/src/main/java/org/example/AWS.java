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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;



import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        // Build the S3 client with the specified region
        s3 = S3Client.builder()
                .region(region1)
                .build();
        sqs = SqsClient.builder()
                .region(region1)
                .build();
        ec2 = Ec2Client.builder()
                .region(region1)
                .build();
        bucketName = "my-bucket"; // Replace with your bucket name
    }

    public static AWS getInstance() {
        if (instance == null) {
            return new AWS();
        }
        return instance;
    }


    //////////////////////////////////////////  EC2

    public boolean isInstanceRunningFromAmi(String ami) {
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
        return response.reservations().stream()
                .flatMap(reservation -> reservation.instances().stream())
                .anyMatch(instance -> ami.equals(instance.imageId()));
    }


    //designed to launch Amazon EC2 instances using a specified Amazon Machine Image (AMI).
    public void runInstanceFromAmiWithScript(String ami, InstanceType instanceType, int min, int max, String script) {
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(instanceType)
                .minCount(min)
                .maxCount(max)
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                // @ADD security feratures
                .build();

        // Launch the instance
        ec2.runInstances(runInstancesRequest);
    }

    //The getAllInstances function is designed to retrieve a list of all EC2 instances in the AWS account.
    public List<Instance> getAllInstances() {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().build();

        DescribeInstancesResponse describeInstancesResponse = null;
        describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

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


    public void downloadFileFromS3(String fileUrl, File outputFile) throws Exception {
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

        // Download the file from S3 and save it locally
        downloadFile(bucketName, objectKey, outputFile);
    }

    private void downloadFile(String bucketName, String objectKey, File outputFile) throws IOException {
        // Build the S3 GetObjectRequest
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        // Get the S3 object as bytes
        ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);

        // Write the bytes to the output file
        try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
            outputStream.write(objectBytes.asByteArray());
        }

        System.out.println("File downloaded successfully to: " + outputFile.getAbsolutePath());
    }

    // The listObjectsInBucket function is designed to list the objects in an Amazon S3 bucket
    // and print information about each object (its key and size).

    public SdkIterable<S3Object> listObjectsInBucket(String bucketName) {
        // Build the list objects request
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(1)
                .build();

        ListObjectsV2Iterable listRes = null;
        listRes = s3.listObjectsV2Paginator(listReq);
        // Process response pages
        listRes.stream()
                .flatMap(r -> r.contents().stream())
                .forEach(content -> System.out.println(" Key: " + content.key() + " size = " + content.size()));

        return listRes.contents();
    }

    public void deleteEmptyBucket(String bucketName) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
        s3.deleteBucket(deleteBucketRequest);
    }

    public void deleteAllObjectsFromBucket(String bucketName) {
        SdkIterable<S3Object> contents = listObjectsInBucket(bucketName);

        Collection<ObjectIdentifier> keys = contents.stream()
                .map(content ->
                        ObjectIdentifier.builder()
                                .key(content.key())
                                .build())
                .toList();

        Delete del = Delete.builder().objects(keys).build();

        DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(del)
                .build();

        s3.deleteObjects(multiObjectDeleteRequest);
    }


    public void deleteBucket(String bucketName) {
        deleteAllObjectsFromBucket(bucketName);
        deleteEmptyBucket(bucketName);
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

    ////////////////////////// hamza and aseel added this  :


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


    // Delete the first line from the file in S3


    public String createEmptyFileInS3(String keyPath) {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyPath)
                    .build();

            // Upload an empty file (zero-byte content)
            s3.putObject(putObjectRequest, RequestBody.fromString(""));

            // Construct the file URL
            String fileUrl = "https://" + bucketName + ".s3." + region1.id() + ".amazonaws.com/" + keyPath;
            System.out.println("Created an empty file at: " + fileUrl);

            return fileUrl;
        } catch (Exception e) {
            e.printStackTrace();
            return null; // Return null in case of an error
        }
    }


    public void deleteFileFromS3(String keyPath) {
        // Create a delete request
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)  // The bucket name, assumed to be set in your class
                .key(keyPath)        // The key (path) of the file to delete
                .build();

        // Delete the file from S3
        s3.deleteObject(deleteObjectRequest);


    }


//////////////////////////////////////////  S3 Bucket URL Handling

    // Function to return the URL of a bucket based on its name
    public String getBucketUrl(String bucketName) {
        // AWS S3 bucket URLs typically follow this format:
        return "https://" + bucketName + ".s3." + region1.id() + ".amazonaws.com";
    }

    // Updated createBucket function to return the bucket URL

    public void deleteInstancesWithLabel(Label label, int workersToDelete) throws InterruptedException {
        // Retrieve all instances with the specified label
        List<Instance> instancesWithLabel = getAllInstancesWithLabel(label);

        // If there are no instances to delete, return
        if (instancesWithLabel.isEmpty()) {
            System.out.println("No instances found with label: " + label);
            return;
        }

        // Limit the number of instances to delete based on the provided count
        int countToDelete = Math.min(instancesWithLabel.size(), workersToDelete);

        System.out.println("Deleting up to " + countToDelete + " instances with label: " + label);

        // Iterate over the instances and delete them
        for (int i = 0; i < countToDelete; i++) {
            String instanceId = instancesWithLabel.get(i).instanceId();
            terminateInstance(instanceId);
            System.out.println("Deleted instance with ID: " + instanceId);
        }

        System.out.println("Successfully deleted " + countToDelete + " instances with label: " + label);
    }


    public List<String> receiveAndDeleteMsg(String queueUrl) {
        // Receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1) // Retrieve up to 1 messages in a single call
                .visibilityTimeout(5)    // Visibility timeout in seconds
                .build();

        // Fetch the messages
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

        // Delete each message after processing it
        for (Message message : messages) {
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteRequest); // Delete the message from the queue
        }

        // Return the message bodies
        return messages.stream()
                .map(Message::body)
                .collect(Collectors.toList());
    }


    public void deleteMessageFromQueue(String queueUrl, Message message) {
        // Retrieve the receipt handle from the message
        String receiptHandle = message.receiptHandle();

        // Create a DeleteMessageRequest
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)  // The URL of the SQS queue
                .receiptHandle(receiptHandle)  // The receipt handle of the message to delete
                .build();

        // Call the deleteMessage method to remove the message from the queue
        sqs.deleteMessage(deleteRequest);
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

// this function for the manager


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
                "aws s3 cp s3://aseeljarfiles/Manager-1.0-SNAPSHOT.zip .\n" +
                "unzip -P Aseel Manager-1.0-SNAPSHOT.zip\n" +
                "java -jar Manager-1.0-SNAPSHOT.jar\n";  // Updated JAR name

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



