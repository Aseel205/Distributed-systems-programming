package org.example;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import software.amazon.awssdk.services.sqs.model.Message;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.UUID;



public class Worker {

    final static AWS aws = AWS.getInstance();
    private static final String Manger_worker_queue_name = "Thread_Worker_queue"; //no use
    private static final String Worker_Manger_queue_name = "Worker_Manager_queue";
    static String responseMessage;
    private static String operation;
    private static String orgPdfUrl;
    private static String localID;
    private static int flag;
    private static String resultFilePath;
    private static String S3ResultUrl;
    private static String N;
    private static String bucketResultsName;
    static String tempDownloadPath = "ameer.pdf";


    public static void main(String[] args) {
        Worker worker = new Worker();
        try {
            worker.processMessage();
        } catch (IOException | InterruptedException e) {
//            System.err.println("An error occurred while processing messages: " + e.getMessage());
        }
    }


    /**
     * Main method to process the message
     */
    public void processMessage() throws IOException, InterruptedException {
        String queueUrl = aws.getQueueUrl(Manger_worker_queue_name);

        while (true) {
            // Check if there are any messages in the queue
            while (aws.getQueueSizeWithOutHiddenMsg(queueUrl) == 0) {
                System.out.println("--------------------There-are-no-messages-------------------");
                Thread.sleep(1000);
            }

            // Receive the message and hide it from the queue for the specified time
            Message message = aws.receiveMessageFromQueue(queueUrl);
            if (message != null) {
            aws.changeMessageVisibility(queueUrl,message,120) ;
                parseMessage(message.body());
                downloadPdf();
                performOperation();
                responseMessage = buildResponseMessage();
                uploadFile();
                sendMessage();
                aws.deleteMessageForever(message, queueUrl);
                System.out.println("Response Message: " + responseMessage);
                cleanupFiles();

            }
        }
    }


    private void parseMessage(String message) {
        try {
            // Split the message by the delimiter "$"
            String[] parts = message.split("\\$");

            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid message format: Must have 6 parts separated by '$'.");
            }

            // Extract operation and original PDF URL
            String operationAndUrl = parts[0].trim();
            String[] operationAndUrlParts = operationAndUrl.split("\\s+", 2);

            operation = operationAndUrlParts[0];  // The operation (e.g., "ToImage", "ToHTML")
            orgPdfUrl = operationAndUrlParts[1];  // The original PDF URL

            localID = parts[1].trim();        // The output file
            N = parts[2].trim();        // The output file
            bucketResultsName = localID;

            // Print parsed values for debugging
            System.out.println("Message parsed successfully: --------------------------New-Message----------------------------");
            System.out.println("Operation: " + operation);
            System.out.println("Original PDF URL: " + orgPdfUrl);
        } catch (Exception e) {
            System.err.println("Error parsing message: " + e.getMessage());
        }
    }



    public static void downloadPdf() {
        HttpURLConnection connection = null;
        try {
            System.out.println("Downloading PDF from URL...");

            // Validate the URL
            URL url;
            try {
                url = new URL(orgPdfUrl);
            } catch (MalformedURLException e) {
                System.err.println("Invalid URL: " + orgPdfUrl);
                flag = 0;
                return;
            }

            // Open connection
            try {
                connection = (HttpURLConnection) url.openConnection();
                connection.setInstanceFollowRedirects(false); // Disable automatic redirection
            } catch (IOException e) {
                System.err.println("Error establishing connection to the URL: " + e.getMessage());
                flag = 0;
                return;
            }

            int responseCode;
            try {
                responseCode = connection.getResponseCode();
            } catch (IOException e) {
                System.err.println("Error getting response from the server: " + e.getMessage());
                flag = 0;
                return;
            }

            // Handle redirects
            if (responseCode == HttpURLConnection.HTTP_MOVED_PERM ||
                    responseCode == HttpURLConnection.HTTP_MOVED_TEMP) {
                String newUrl = connection.getHeaderField("Location");
                System.out.println("Redirected to: " + newUrl);

                try {
                    url = new URL(newUrl);
                    connection = (HttpURLConnection) url.openConnection();
                    responseCode = connection.getResponseCode();
                } catch (MalformedURLException e) {
                    System.err.println("Invalid redirect URL: " + newUrl);
                    flag = 0;
                    return;
                } catch (IOException e) {
                    System.err.println("Error following redirect: " + e.getMessage());
                    flag = 0;
                    return;
                }
            }

            // Check final response
            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (InputStream in = connection.getInputStream()) {
                    Files.copy(in, Paths.get(tempDownloadPath), StandardCopyOption.REPLACE_EXISTING);
                    System.out.println("PDF downloaded successfully to: " + tempDownloadPath);
                    flag = 1;
                } catch (IOException e) {
                    System.err.println("Error saving PDF file: " + e.getMessage());
                    flag = 0;
                }
            } else {
                System.err.println("Failed to download PDF. HTTP Response Code: " + responseCode);
                flag = 0;
            }
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            flag = 0;
        } finally {
            // Clean up the connection
            if (connection != null) {
                connection.disconnect();
            }
        }
    }


    /**
     * Step 3: Perform the specified operation
     */
    private static void performOperation() throws IOException {
        if (flag == 1) {
            switch (operation) {
                case "ToImage":
                    resultFilePath = convertPdfToImage(tempDownloadPath);
                    break;
                case "ToHTML":
                    resultFilePath = convertPdfToHtml(tempDownloadPath); // I changed this;;
                    break;
                case "ToText":
                    resultFilePath = convertPdfToText(tempDownloadPath);
                    break;
                default:
                    System.err.println("Unsupported operation: " + operation);
                    flag = 0;

            }
        }
    }

    /**
     * Converts the first page of the given PDF file to a PNG image.
     *
     * @param pdfPath The path to the PDF file.
     * @return The path to the generated PNG file.
     * @throws IOException If an error occurs while processing the PDF.
     */
    private static String convertPdfToImage(String pdfPath) throws IOException {
        try (PDDocument document = PDDocument.load(new File(pdfPath))) {
            PDFRenderer renderer = new PDFRenderer(document);
            BufferedImage image =null;
            try {
                image = renderer.renderImageWithDPI(0, 300); // Render first page at 300 DPI
            }
            catch (IOException e){
                System.err.println("Failed to extract image from PDF: " + e.getMessage());
                flag = 0;
                deleteFile(tempDownloadPath);
                return "";
            }
            // Generate a unique filename for the image
            String outputPath = generateUniqueFilename("output_image", "png");
            File outputFile = new File(outputPath);

            // Save the image
            ImageIO.write(image, "PNG", outputFile);
            System.out.println("Converted PDF to image at: " + outputPath);
            return outputPath; // Return the file path of the generated image

        } catch (IOException e) {
            System.err.println("can't open the file :( " + e.getMessage());
            flag = 0;
            return "";
        }

    }

    /**
     * Extracts text from the first page of the given PDF file and saves it to a text file.
     *
     * @param pdfPath The path to the PDF file.
     * @return The path to the generated text file.
     * @throws IOException If an error occurs while processing the PDF.
     */
    private static String convertPdfToText(String pdfPath) throws IOException {
        File pdfFile = new File(pdfPath);

        // Check if the input PDF file exists
        if (!pdfFile.exists() || !pdfFile.isFile()) {
            throw new IOException("PDF file not found: " + pdfPath);
        }

        try (PDDocument document = PDDocument.load(pdfFile)) {
            PDFTextStripper textStripper = new PDFTextStripper();

            // Set the range to extract only the first page
            textStripper.setStartPage(1);
            textStripper.setEndPage(1);

            String pdfText = "";
            try {
                pdfText = textStripper.getText(document);
            }
            catch (IOException e){
                System.err.println("Failed to extract text from PDF: " + e.getMessage());
                flag = 0;
                deleteFile(tempDownloadPath);
                return "";
            }


            // Generate a unique filename for the text file
            String outputPath = generateUniqueFilename("output", "txt");
            File outputFile = new File(outputPath);

            // Save the extracted text to a file
            try (java.io.FileWriter writer = new java.io.FileWriter(outputFile)) {
                writer.write(pdfText);
            }

            System.out.println("Converted PDF to Text at: " + outputPath);
            System.out.println("Converted first page of PDF to Text at: " + outputPath);

            return outputPath; // Return the file path of the generated text file

        } catch (IOException e) {
            System.err.println("can't open the file :( " + e.getMessage());
            flag = 0;
            return "";
        }
    }

    /**
     * Converts the first page of the given PDF file to a basic HTML file.
     *
     * @param pdfPath The path to the PDF file.
     * @return The path to the generated HTML file.
     * @throws IOException If an error occurs while processing the PDF.
     */


    private static String convertPdfToHtml(String pdfPath) throws IOException {
        File pdfFile = new File(pdfPath);

        // Validate input PDF file
        if (!pdfFile.exists() || !pdfFile.isFile()) {
            throw new IOException("PDF file not found: " + pdfPath);
        }

        try (PDDocument document = PDDocument.load(pdfFile)) {
            PDFTextStripper textStripper = new PDFTextStripper();

            // Set range to extract only the first page
            textStripper.setStartPage(1);
            textStripper.setEndPage(1);

            // Extract text from the first page
            String pdfText;
            try {
                pdfText = textStripper.getText(document);
            } catch (IOException e) {
                System.err.println("Failed to extract text from PDF: " + e.getMessage());
                deleteFile(tempDownloadPath);
                return ""; // Handle the error by returning an empty string
            }

            // Simple HTML generation
            String htmlContent = "<html><body>" + pdfText.replace("\n", "<br>") + "</body></html>";

            // Generate a unique filename for the HTML file
            String outputPath = generateUniqueFilename("output", "html");
            File outputFile = new File(outputPath);

            // Save the HTML content to a file
            try (java.io.FileWriter writer = new java.io.FileWriter(outputFile)) {
                writer.write(htmlContent);
            }

            System.out.println("Converted first page of PDF to HTML at: " + outputPath);

            return outputPath; // Return the file path of the generated HTML file

        } catch (IOException e) {
            System.err.println("can't open the file :( " + e.getMessage());
            return "";
        }
    }


    /**
     * Step 4: Create the response message
     */
    private static String buildResponseMessage() {
        return String.format("%s$%s$%s$%s$%s$%d", operation, orgPdfUrl, S3ResultUrl, localID, N, flag);
    }

    /**
     * Step 5: Cleanup temporary files
     */
    private static void cleanupFiles() {
        if (flag == 1) {
            deleteFile(tempDownloadPath);
            deleteFile(resultFilePath);
        }
    }


    private static String generateUniqueFilename(String baseName, String extension) {
        String uuid = UUID.randomUUID().toString(); // Generate a unique identifier
        return baseName + "_" + uuid + "." + extension;
    }


    private static void deleteFile(String filePath) {
        try {
            Files.deleteIfExists(Paths.get(filePath));
            System.out.println("Deleted temporary file: " + filePath);
        } catch (IOException e) {
            System.err.println("Error deleting file: " + e.getMessage());
        }
    }

    private static void sendMessage() {
        aws.sendMsg(aws.getQueueUrl(Worker_Manger_queue_name), responseMessage);
    }

    public static void uploadFile() {
        if (flag == 1) {
            try {
                S3ResultUrl = aws.uploadFileToS3(resultFilePath, bucketResultsName);
                System.out.println("File uploaded successfully to S3! URL: " + S3ResultUrl);
            } catch (Exception e) {
                System.err.println("Error uploading file to S3: " + e.getMessage());
            }
        } else resultFilePath = "";

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
                "aws s3 cp s3://workeroutput/LocalApp-1.0-SNAPSHOT.jar ./LocalApp-1.0-SNAPSHOT.jar\n" +
                "java -jar LocalApp-1.0-SNAPSHOT.jar\n";
    }

}