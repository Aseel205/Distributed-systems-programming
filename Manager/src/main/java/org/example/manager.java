
package org.example;

import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;

import static java.lang.Thread.sleep;


public class manager {

    final static AWS aws = AWS.getInstance();
    // URLS
    private String App_Manger_queue_Url;
    private String Manger_Thread_queue_Url;
    private String Thread_worker_queue_Url;
    private String Worker_manger_queue_Url;
    private String SummaryFileBucketURL;


    // Names
    private final String Worker_manger_queue_name = "Worker_Manager_queue";
    private final String SummaryFiles_BucketName = "aseelbiadse1234";
    private String app_Manger_queue_name = "App_manager_queue";
    private String manger_Thread_queue_name = "Manger_Thread_queue";
    private String thread_worker_queue_name = "Thread_Worker_queue";

    // hashTables
    private static HashMap<String, File> inputFileHashTable;
    private static HashMap<String, Integer> workingHashTable;
    private static HashMap<String, File> outputFileHashTable;
    private static HashMap<String, Integer> IdTo_AddedWorkers;

    // counters
    private boolean received_terminateMsg = false;
    private boolean stopThreads = false;
    private final Integer maxWorkers = 8;   // 8 worker + 1 manager  = 9 instances
    private Integer ActiveWorkerNum = 0;
    private Integer messagePerApp  = 50  ;   //  this variable can be changed according to the manager use

    // synchronize  keys
    private final Object ActiveWorkerNum_key = new Object();
    private final Object workingHashTable_key = new Object();
    AWS.Label worker = AWS.Label.Worker;

    // threads
    private Thread Reception_Thread;
    private Thread ManagerWorkerQueue_Thread;
    private Thread WorkerManager_Thread;
    private Integer counter  = 0 ;


    public static void main(String[] args) {
        manager mymanager = new manager();
        mymanager.start();
    }


    public void start() {
        System.out.println("the manger has started...");

        // initialize the queues and the fields
        aws.createBucket(SummaryFiles_BucketName);
        SummaryFileBucketURL = aws.getBucketUrl(SummaryFiles_BucketName);
        App_Manger_queue_Url = aws.getQueueUrl(app_Manger_queue_name);

        // initialize the queue
        Manger_Thread_queue_Url = aws.createQueue(manger_Thread_queue_name);
        Thread_worker_queue_Url = aws.createQueue(thread_worker_queue_name);
        Worker_manger_queue_Url = aws.createQueue(Worker_manger_queue_name);

        // initialize the hashtables
        inputFileHashTable = new HashMap<>();
        workingHashTable = new HashMap<>();
        outputFileHashTable = new HashMap<>();
        IdTo_AddedWorkers = new HashMap<>();


        // initialize the threads
        Reception_Thread = new Thread(() -> {
            try {
                receive_new_application();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });
        ManagerWorkerQueue_Thread = new Thread(() -> {
            try {
                try {
                    send_message_to_worker();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        WorkerManager_Thread = new Thread(() -> {
            try {
                try {
                    read_message_from_worker();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Reception_Thread.start();
        ManagerWorkerQueue_Thread.start();
        WorkerManager_Thread.start();

        System.out.println("the main function has started all the threads");

    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private void receive_new_application() throws Exception {

        while (!received_terminateMsg) {

            //thread will check for new message every 20 seconds
            while (aws.getQueueSize(App_Manger_queue_Url) == 0) {
                sleep(20 * 1000);
               // ReviveWorkersIfNeeded() ;
            }


            List<Message> messages = aws.receiveMsg(App_Manger_queue_Url);

            // Iterate over retrieved messages
            for (Message message : messages) {
                String messageBody = message.body();

                // terminate message
                if (messageBody.equals("TERMINATE!!")) {
                    System.out.println("the manager received terminate message  .....!!!");
                    received_terminateMsg = true;
                }

                // regular message
                else {

                    int AddWorkers = appmsg_toN(messageBody);
                    String applicationId = appmsg_toApplicationID(messageBody);

                    // add new workers if needed
                    synchronized (ActiveWorkerNum_key) {
                        int value = 0;
                        for (int i = AddWorkers; ActiveWorkerNum < maxWorkers && i > 0; i--) {
                            ActiveWorkerNum++;
                            value++;
                        }
                        aws.runInstanceFromAMI(aws.AMI_ID, value, worker);
                        IdTo_AddedWorkers.put(applicationId, value);
                    }

                    // initialize  the files
                    aws.downloadAndAddToHashtable(appmsg_toInputURl(messageBody), applicationId, inputFileHashTable);
                    outputFileHashTable.put(applicationId, new File("outputFiles" + applicationId));  //  this may cause error
                    workingHashTable.put(applicationId, 0);

                    aws.sendMsg(Manger_Thread_queue_Url, messageBody);
                    System.out.println("Send Message :from manager to thread ");
                }
                // delete the message from the queue
                try {
                    aws.sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(App_Manger_queue_Url)
                            .receiptHandle(message.receiptHandle())  // Use the current message's receipt handle
                            .build());
                    System.out.println("Message deleted from App_Manger_queue.");
                } catch (SqsException e) {
                    System.err.println("Error deleting message: " + e.awsErrorDetails().errorMessage());
                }

            }
        }
    }


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private void send_message_to_worker() throws InterruptedException, IOException {

        while (true) {

            while (!stopThreads && aws.getQueueSize(Manger_Thread_queue_Url) == 0) {
                sleep(10 * 1000);
            }
            if (stopThreads)
                break;


            // dealing with the message
            List<Message> managerThreadMessages = aws.receiveMsg(Manger_Thread_queue_Url);

            // Iterate over messages
            for (Message message : managerThreadMessages) {
                String msgBody = message.body();

                // Get the information from the message.
                String applicationId = appmsg_toApplicationID(msgBody);
                String inputPath = appmsg_toInputURl(msgBody);
                int n = appmsg_toN(msgBody);

                System.out.println("for debug ////////////////////////////////////////////////////////////");
                System.out.println("applicationId  : " + applicationId);
                System.out.println("inputPath   : " + inputPath);
                System.out.println(" n is : " + n);
                File file = inputFileHashTable.get(applicationId);
                System.out.println( "number of lines in file " +  file.getAbsolutePath() +  getNumberOfLines(file));
                System.out.println("//////////////////////////////////////////////////////////////////////");

                // temp variables
                String firstLine = "";
                int messagesSent = 0;
                for (int i = 0; i < messagePerApp && file.length()!=0; i++) {
                    firstLine = readAndRemoveFirstLine(file);
                    aws.sendMsg(Thread_worker_queue_Url, firstLine + "$" + applicationId + "$" + n);
                    System.out.println("Send Message : The manager-worker thread sent message to the worker  and this the line  :" + firstLine);
                    messagesSent++;


                }
                System.out.println("/////////////////////////////////////////////////////////////////////////////");

                // updating the working file
                synchronized (workingHashTable_key) {
                    int value = workingHashTable.get(applicationId);
                    workingHashTable.remove(applicationId);
                    workingHashTable.put(applicationId, value + messagesSent);
                }

                // delete the file and the message
                if (file.length() == 0) {
                    try {
                        aws.deleteMessageForever(message, Manger_Thread_queue_Url);
                        System.out.println("all the application message was sent to the workers ");

                    } catch (SqsException e) {
                        System.err.println("Error deleting message: " + e.awsErrorDetails().errorMessage());
                    }
                    // delete the input file from the hashtable and from the manager

                        inputFileHashTable.remove(applicationId);

                }


            }
        }
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private void read_message_from_worker() throws InterruptedException, IOException {

        while (true) {

            // sleep & check
            while (!received_terminateMsg && aws.getQueueSize(Worker_manger_queue_Url) == 0) {
                sleep(1000 * 10);
            }


            List<Message> WorkerManagerMessages = aws.receiveMsg(Worker_manger_queue_Url);

            // Iterate over messages
            for (Message message : WorkerManagerMessages) {
                String message_body = message.body();

                // check if the worker send message twice
                if (outputFileHashTable.containsKey(workermessage_toId(message_body))) {

                    String newLine = "";   // temp variable

                    // Add a new line to the output file based on the flag in the message body
                    if (workermessage_toFlag(message_body).compareTo("1") == 0) {
                        newLine = "<" + workermessage_toOperation(message_body) + " " +
                                workermessage_toOriginalPdfUrl(message_body) + " " +
                                workermessage_toOutputPdfUrl(message_body);
                    } else {
                        newLine = "<" + workermessage_toOperation(message_body) + " " +
                                workermessage_toOriginalPdfUrl(message_body) + " " +
                                "pdf file is not available !!";
                    }

                    String applicationId = workermessage_toId(message_body);
                    // add the line to the output file
                    if (outputFileHashTable.containsKey(applicationId)) {
                        File outputfile = outputFileHashTable.get(applicationId);
                        addLineToFile(outputfile, newLine);
                    } else {
                        System.out.println(" the worker send an extra message for the application");  //  we will not reach this line
                    }


                    counter++;
                    System.out.println("this  is THE " + counter + " message :)");
                    System.out.println(" NewLine : NewLine was added to " + applicationId + " outputfile");

                    // helper counter for the messages
                    if(counter == 10000) {
                        System.out.println(" the counter reached 10000");
                        counter = 0 ;
                        System.out.println(" the counter was rested to 0");
                    }


                    // Delete the line from the working file
                    synchronized (workingHashTable_key) {
                        int value = workingHashTable.get(applicationId);
                        workingHashTable.remove(applicationId);
                        workingHashTable.put(applicationId, value - 1);
                    }


                    // Check if we should send the message to the local application
                    if (IsThisLastMessage(applicationId)) {
                        // upload the file to s3
                        String outputFileUrl = aws.uploadFileToS3(outputFileHashTable.get(applicationId).getPath(), SummaryFiles_BucketName); //
                        String queueUrl = aws.getQueueUrl(workermessage_toId(message_body));
                        aws.sendMsg(queueUrl, outputFileUrl);

                        System.out.println("Send message: the message was sent to the application.");
                        File uneededFile = outputFileHashTable.remove(applicationId);
                        uneededFile.delete();

                        workingHashTable.remove(applicationId);

                        // remove workers
                        synchronized (ActiveWorkerNum_key) {
                            int AddedWorkers = IdTo_AddedWorkers.get(applicationId);
                            aws.deleteInstancesWithLabel(AddedWorkers, aws.AMI_ID, worker);
                            ActiveWorkerNum = ActiveWorkerNum - IdTo_AddedWorkers.get(applicationId);
                            IdTo_AddedWorkers.remove(applicationId);
                        }

                    }


                }
                // Explicitly delete the current message from the queue
                try {
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(Worker_manger_queue_Url)
                            .receiptHandle(message.receiptHandle())  // Use the current message's receipt handle
                            .build();
                    aws.sqs.deleteMessage(deleteRequest);  // Delete the message from the queue
                    System.out.println("Message deleted from Worker_manger_queue.");
                } catch (SqsException e) {
                    System.err.println("Error deleting message: " + e.awsErrorDetails().errorMessage());
                }

            }

            // check if to shut down  the program
            if (received_terminateMsg && inputFileHashTable.isEmpty() && outputFileHashTable.isEmpty()) {
                stopThreads = true;
                System.out.println("the threads will stop and the manager will start the clean up");
                Thread cleanup_thread = new Thread(() -> cleanup());
                cleanup_thread.start();
                break;
            }


        }

    }

    ////////////////// clean up
    public void cleanup() {

        System.out.println("the manager have start the clean up");
        try {
            sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        // delete the workers if needed
        if (ActiveWorkerNum > 0) {
            aws.deleteInstancesWithLabel(ActiveWorkerNum, aws.AMI_ID, worker);
        }

        // delete the queues
        aws.deleteQueue(App_Manger_queue_Url);
        aws.deleteQueue(Thread_worker_queue_Url);
        aws.deleteQueue(Worker_manger_queue_Url);
        aws.deleteQueue(Manger_Thread_queue_Url);


        // delete the bucket of the SummaryFiles
        aws.deleteBucket(SummaryFiles_BucketName);

        System.out.println("Cleanup completed");
        System.out.println(" the manager will shut down ...");
        aws.deleteInstancesWithLabel(1, aws.AMI_ID,  AWS.Label.Manager);

    }


    // this function cased the AWS to band the account se we decided not to use it.

    public void ReviveWorkersIfNeeded() {

            synchronized (ActiveWorkerNum_key){
            int AliveWorkers = aws.countRunningInstancesFromAmi(aws.AMI_ID)-1 ;   // this line what cause the error ...
            if(AliveWorkers < ActiveWorkerNum  )
                aws.runInstanceFromAMI(aws.AMI_ID, (ActiveWorkerNum - AliveWorkers) ,  worker);
            }

    }



// extra minor functions

//App message decoder
    private String[] parseAppMessage(String msgBody) {
        return msgBody.split("\\$");
    }

    private String appmsg_toApplicationID(String msgBody) {
        String[] parts = parseAppMessage(msgBody);
        return parts[1]; // applicationId
    }

    private int appmsg_toN(String msgBody) {
        String[] parts = parseAppMessage(msgBody);
        return Integer.parseInt(parts[2]); // n
    }

    private String appmsg_toInputURl(String msgBody) {
        String[] parts = parseAppMessage(msgBody);
        return parts[0]; // inputFileUrl
    }


// Worker message decoder

    // Helper method to split the message body by delimiter '$'
    private String[] parseMessage(String msgBody) {
        return msgBody.split("\\$");
    }

    private String workermessage_toId(String msgBody) {
        String[] parts = parseMessage(msgBody);
        return parts[3]; // localID
    }

    private String workermessage_toOriginalPdfUrl(String msgBody) {
        String[] parts = parseMessage(msgBody);
        return parts[1]; // orgPdfUrl
    }

    private String workermessage_toOutputPdfUrl(String msgBody) {
        String[] parts = parseMessage(msgBody);
        return parts[2]; // outputFile
    }

    private String workermessage_toFlag(String msgBody) {
        String[] parts = parseMessage(msgBody);
        return parts[5]; // flag
    }

    private String workermessage_toOperation(String msgBody) {
        String[] parts = parseMessage(msgBody);
        return parts[0]; // operation
    }

    private boolean IsThisLastMessage(String id) {

        return (!inputFileHashTable.containsKey(id)) && workingHashTable.get(id) ==0 ;
    }

    public static String readAndRemoveFirstLine(File file) throws IOException {
        List<String> lines = Files.readAllLines(file.toPath());

        if (lines.isEmpty()) {
            throw new IOException("The file is empty.");
        }

        String firstLine = lines.get(0);

        lines.remove(0);

        Files.write(file.toPath(), lines);

        return firstLine;
    }


    public static void addLineToFile(File file, String line) throws IOException {
        // Use BufferedWriter to append the line to the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            writer.write(line);
            writer.newLine(); // Add a new line after the string
        }
    }


        public static int getNumberOfLines (File file){
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


