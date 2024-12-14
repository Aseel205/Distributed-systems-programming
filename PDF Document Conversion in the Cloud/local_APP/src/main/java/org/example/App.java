package org.example;
import java.io.*;
import java.util.List;
import java.util.UUID;

import software.amazon.awssdk.services.sqs.model.Message;


import static java.lang.Thread.sleep;


public class App {

    final static AWS aws = AWS.getInstance();

    // queues name
    private static final String App_Manager_Queue_Name = "App_manager_queue";
    private static String manager_App_queue_name;      // = appliction id

    // queues URL
    private static String App_Manager_Queue_Url;
    private static String manager_app_Queue_Url;

    // helper variables
    private static boolean shouldTerminate;
    private static Integer n;
    private static String inputFileName;
    private static String outputFileName;
    private static String AppBucketName; //  =  application id
    private static String inputFileUrl;
    static AWS.Label manager = AWS.Label.Manager;




    public static void main(String[] args) {

        setup(args);
    }


    public static void setup(String[] args) {

        System.out.println("Starting the application....");
        String relativePath = "../src/main/resources/";



        inputFileName = args[0];
        outputFileName = args[1];
        n = Integer.parseInt(args[2]);

        // create the files
        File inputFile = new File(relativePath+ inputFileName);
        File htmlOutputFile = new File(relativePath + outputFileName);
        File NotHtmlOutputFile = new File(relativePath + "NotHtmlOutputFile");

        try {
            NotHtmlOutputFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        //starting the parameters
        shouldTerminate = args.length > 3 && "terminate".equalsIgnoreCase(args[3]);
        String applicationId = UUID.randomUUID().toString();
        AppBucketName = applicationId;
        manager_App_queue_name = applicationId;

        aws.createBucket(AppBucketName);

        // start the manager if needed
        if (!aws.isInstanceRunningFromAmi(aws.AMI_ID)) {
            System.out.println("the app will creat a manager instance");
            aws.runInstanceFromAMI(aws.AMI_ID , 1 , manager);
            System.out.println("the manager was created");
        }



        // upload the file to S3
        try {
            inputFileUrl = aws.uploadFileToS3(inputFile.getPath(), AppBucketName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        // creat the App_Manager_Queue  if it doesn't exist (the queue from the app to the manager )
        if (!aws.isQueueExist(App_Manager_Queue_Name)) {
            App_Manager_Queue_Url = aws.createQueue(App_Manager_Queue_Name);
        }

        // creat the manger_APP_queue (the queue from the manager to the app)
        manager_app_Queue_Url = aws.createQueue(manager_App_queue_name);

        int neededWorkers = Math.max(getNumberOfLines(inputFile) / n  ,  1 ) ;

        // send the message to the manger
        aws.sendMsg(aws.getQueueUrl(App_Manager_Queue_Name), inputFileUrl + "$" + applicationId + "$" + neededWorkers);


        //  sleep and check until we get the answer from the manager
        while (aws.getQueueSize(aws.getQueueUrl(manager_App_queue_name)) == 0) {
            try {
                System.out.println("the App is  going to sleep for 5 seconds");
                sleep(1000 * 5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // receive the manager message
        System.out.println("ReceiveMessage : the message was received from the manager");

        List<Message> managerMessage = aws.receiveMsg(applicationId);

        for (Message message : managerMessage) {
            String message_body = message.body();
            System.out.println("for debug ///////////////");
            System.out.println("message body : " + message_body);
            System.out.println("/////////////////////////");


            try {
                aws.downloadFileFromS3(message_body, NotHtmlOutputFile);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }



            try {
                convertFileToHtml(NotHtmlOutputFile, htmlOutputFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }


        if (shouldTerminate) {
            aws.sendMsg(aws.getQueueUrl(App_Manager_Queue_Name), "TERMINATE!!");
            System.out.println(" a  TERMINATE!!  message was sent to the manager");



        }

        System.out.println("the application has shutdown,the application will start the cleanup....");
        cleanup();

    }


    public static void cleanup() {
        System.out.println("the application has start the clean up !!");
        aws.deleteQueue(manager_app_Queue_Url);
        System.out.println("the application has finished the clean up!!");
        System.out.println("exit...");

    }


    public static void convertFileToHtml(File inputFile, File outputFile) throws IOException {
        try (
                BufferedReader reader = new BufferedReader(new FileReader(inputFile));
                BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))
        ) {
            writer.write("<!DOCTYPE html>\n<html>\n<head>\n<title>Converted File</title>\n</head>\n<body>\n<pre>\n");

            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine(); // Preserve line breaks
            }

            writer.write("</pre>\n</body>\n</html>");
        }
    }


    public static int getNumberOfLines(File file) {
        int lineCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            while (br.readLine() != null) {
                lineCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lineCount;
    }
}





/// and that's it  ........