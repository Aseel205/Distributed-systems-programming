# README

## Team Members
- **Name:** Aseel Biadsi  
  **ID:** 213758758  
- **Name:** Hamza Mjadly  
  **ID:** 325043552  

---


AWS EC2 Image for Java and AWS Tools.

AMI ID: ami-0829e00d96164fbc9.

Pre-configured with:

- Java JDK/JRE for Java development.
- AWS CLI for seamless AWS service integration.
- Optimized for fast and consistent deployments.

---


## How to Run the Project
1. Create the JAR files using `mvn clean package`.
2. Upload the JAR files to S3.
	- Configure AWS CLI:
		```bash
 		aws configure
 	(this will ask you for the credentials)
	- Create an S3 Bucket  (in this case "aseeljarfiles " was our bucketName ) :
		```bash
 		aws s3 mb s3://aseeljarfiles 
	- Upload the JAR Files (using the absolute path, the path will change from machine to other) :
  		```bash
 	 	aws s3 cp "C:\Users\aseel\OneDrive\Desktop\mevazrot\Manager\target\A.zip" s3://aseeljarfiles/
   
  		aws s3 cp "C:\Users\aseel\OneDrive\Desktop\mevazrot\DSP_WORKER 2\target\Hmza_APP-1.0-SNAPSHOT.zip" s3://aseeljarfiles/

4. Activate the application using:  
   ```bash
   java -jar untitled-1.0-SNAPSHOT.jar "inputfilename" "outputfilename" "n" "terminate - if needed"

5.	(Optional) Delete the JAR files and output files from S3.


---


## How the Program Works

Program Flow:

1.	Local App:
	-	Creates:
	    -	A Manager instance.
    	-	A dedicated bucket (for input and output) named after its ID.
    	-	Its own queue to communicate with the Manager.
    	-	Another queue called App -> Manager.
	-	Uploads the input file to S3.
	-	Sends its ID, the input file URL, and the required number of Workers to the App -> Manager queue.
2.	Manager:
	-	Starts running and initializes:
        -	A Manager -> Worker queue.
        -	A Worker -> Manager queue.
	-	Processes messages from the App -> Manager queue.
	-	Downloads the file from the URL in the message.
	-	Creates Worker instances based on the number requested by the App.
	-	Sends the content of the input file (line by line) along with the App ID to the Manager -> Worker queue.
3.	Worker:
	-	Processes messages from the Manager -> Worker queue.
	-	Downloads the specified PDF file.
	-	Converts the first page of the PDF file.
	-	Uploads the converted file to the bucket associated with the App ID.
	-	Sends the converted file URL back to the Worker -> Manager queue.
4.	Final Steps:
	-	The Manager:
	    -	Collects the URLs from the Worker -> Manager queue.
	    -	Creates and uploads a summary file to S3.
	    -	Notifies the App about the summary file location.
	    -	Manages termination signals and ensures optimal Worker usage.
	    -	Handles failures by reviving Workers as needed.
	-	The App:
	  -	Receives the summary file location and downloads it.


---


## EC2 Instances Used

-	Instance Type: T2.micro.


---


## How much time it took our program to finish working in the input file?

-	First Input File:
      -	size: 100 lines.
      - Time Taken: 2 minutes.
-	Second Input File:
      -	size: 2500 lines.
      - Time Taken: 7 minutes.


---


## Did you think for more than 2 minutes about security?

 credintials are hidden in nano file : ~/.aws/credintials, sending them to manager is by accessing the file only.

___


## What is the n we used & why?

 We used n = 10 to balance system performance and workload, ensuring efficient task distribution without overloading resources.


 ---


 
## Did you think about scalability? Will your program work properly when 1 million clients connected at the same time? How about 2 million? 1 billion?

 Yes, our program runs in parallel for each Local App. Each Local App has its own bucket and queue, so no merging of tasks occurs.
Additionally, we dynamically increase the number of Workers based on the Local Apps’ input file size and their selected n.


---


## What about persistence? What if a node dies? What if a node stalls for a while? Have you taken care of all possible outcomes in the system? Think of more possible issues that might arise from failures. What did you do to solve it? What about broken communications?

If a Worker dies or stalls, the Manager revives a new Worker to complete the task.
We use the visibility timeout mechanism to ensure no messages are lost.


---



## Threads in your application, when is it a good idea? When is it bad?

-	Good Idea: When used to divide tasks among threads, ensuring efficient performance.
-	Bad Idea: Overusing threads can lead to unnecessary overhead.


---



## Did you run more than one client at the same time?

Yes.


---


## Do you understand how the system works?

Yes.


---


## Did you manage the termination process?

Yes.


---


## Did you take in mind the system limitations that we are using?

Yes, we used a maximum of 8 instances and fewer than 32 threads, staying within the system’s constraints.


---


## Are all your workers working hard? Or some are slacking? Why?

Yes, all Workers are equally engaged, starting and completing tasks in synchronization, ensuring balanced effort distribution.


---


## Is your manager doing more work than he’s supposed to?

No, the Manager only facilitates communication and task delegation. The actual processing is handled by the Workers.


---


## Have you made sure each part of your system has properly defined tasks? Did you mix their tasks?

Yes, every part has clearly defined tasks with no overlap.


---


## Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another?

Yes, distributed means tasks are divided across multiple instances to run in parallel. There are no unnecessary waits in the system.
