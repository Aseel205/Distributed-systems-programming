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



import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class AWS {
    public final String AMI_ID = "ami-0829e00d96164fbc9";
    public Region region = Region.US_EAST_1;
    protected final S3Client s3;
    protected final SqsClient sqs;
    protected final Ec2Client ec2;
    protected final String bucketName;
    protected static AWS instance = null;

    private AWS() {
        s3 = S3Client.builder().region(region).build();
        sqs = SqsClient.builder().region(region).build();
        ec2 = Ec2Client.builder().region(region).build();
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
    public void runInstanceFromAMI(String ami) {
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(InstanceType.T2_MICRO)
                .minCount(1)
                .maxCount(5) // todo decide what to put here
                .build();

        // Launch the instance
        ec2.runInstances(runInstancesRequest);
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

//    public String uploadFileToS3(String bucketName, String keyPath, File file) throws Exception {
//        System.out.printf("Start upload: %s, to bucket: %s\n", file.getName(), bucketName);
//
//        PutObjectRequest req =
//                PutObjectRequest.builder()
//                        .bucket(bucketName) // Use the bucketName passed to the function
//                        .key(keyPath)
//                        .build();
//
//        // Assume s3 is an initialized instance of S3Client
//        S3Client s3 = S3Client.create(); // Initialize S3 client
//        s3.putObject(req, file.toPath()); // Upload file to the specified bucket
//
//        // Return the S3 path of the uploaded file
//        return "s3://" + bucketName + "/" + keyPath;
//    }


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
        //System.out.println("Queue URL: " + queueUrl);
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

    public int getQueueSizeWithOutHiddenMsg(String queueUrl) {
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED
                )
                .build();

        GetQueueAttributesResponse queueAttributesResponse = sqs.getQueueAttributes(getQueueAttributesRequest);
        Map<QueueAttributeName, String> attributes = queueAttributesResponse.attributes();

        // Return the count of messages excluding those that are currently hidden (in visibility timeout)
        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED));
    }



    ///////////////////////

    public enum Label {
        Manager,
        Worker
    }

    ////////////////////////// hamza and aseel added this  :


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
        try {
            messages.wait();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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

    /**
     * Reads the first line from a file stored in S3.
     *
     * @param keyPath The key (path) of the file in the S3 bucket.
     * @return The first line of the file.
     */
    public String readFirstLineFromS3(String keyPath) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        String firstLine = null;
        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            String content = new String(objectBytes.asByteArray());
            firstLine = content.split("\n")[0]; // Get the first line
        } catch (Exception e) {
            e.printStackTrace();
        }
        return firstLine;
    }

    /**
     * Deletes the first line from a file stored in S3.
     *
     * @param keyPath The key (path) of the file in the S3 bucket.
     */
    public void deleteFirstLineFromS3(String keyPath) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            String content = new String(objectBytes.asByteArray());

            // Split the file content into lines
            String[] lines = content.split("\n");

            if (lines.length <= 1) {
                System.out.println("File is empty after deleting the first line.");
                return;
            }

            // Remove the first line
            String updatedContent = String.join("\n", Arrays.copyOfRange(lines, 1, lines.length));

            // Upload the updated content back to S3
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyPath)
                    .build();

            s3.putObject(putObjectRequest, RequestBody.fromString(updatedContent));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Adds a line to the end of a file stored in S3.
     *
     * @param keyPath The key (path) of the file in the S3 bucket.
     * @param newLine The line to be appended to the file.
     */
    public void addLineToS3(String keyPath, String newLine) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            String content = new String(objectBytes.asByteArray());

            // Append the new line
            String updatedContent = content.isEmpty() ? newLine : content + "\n" + newLine;

            // Upload the updated content back to S3
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyPath)
                    .build();

            s3.putObject(putObjectRequest, RequestBody.fromString(updatedContent));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createEmptyFileInS3(String keyPath) {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyPath)
                    .build();

            // Upload an empty file (zero-byte content)
            s3.putObject(putObjectRequest, RequestBody.fromString(""));
            System.out.println("Created an empty file at: s3://" + bucketName + "/" + keyPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<String> receiveAndDeleteMsg(String queueUrl) {
        // Receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1) // Retrieve up to 1 messages in a single call
                .visibilityTimeout(20)    // Visibility timeout in seconds
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


    public Message receiveMSGAnd_HideIt(String queueUrl, int visibilityTimeoutSeconds) {
        // Receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)  // Retrieve up to 1 message in a single call
                .visibilityTimeout(visibilityTimeoutSeconds)  // Set visibility timeout for a specific period
                .build();

        // Fetch the messages
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        return messages.isEmpty() ? null : messages.get(0);  // Use get(0) for List
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

    public Message receiveMessageFromQueue(String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)  // Limit to 1 message
                .waitTimeSeconds(20)      // Wait time (long polling)
                .build();

        ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(receiveMessageRequest);

        return receiveMessageResponse.messages().isEmpty() ? null : receiveMessageResponse.messages().get(0);
    }
    public void changeMessageVisibility(String queueUrl, Message message, int visibilityTimeoutSeconds) {
        if (message == null) {
            System.out.println("No message provided to change visibility.");
            return;
        }

        try {
            sqs.changeMessageVisibility(builder -> builder
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .visibilityTimeout(visibilityTimeoutSeconds) // Set custom timeout in seconds
            );
            System.out.println("Visibility timeout changed to " + visibilityTimeoutSeconds + " seconds.");
        } catch (Exception e) {
            System.err.println("Failed to change visibility timeout: " + e.getMessage());
        }
    }




    // Delete a message from the SQS Queue after processing
    public void deleteMessage(String queueUrl, String receiptHandle) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();

        sqs.deleteMessage(deleteMessageRequest);
        System.out.println("Message deleted from the queue.");
    }


}