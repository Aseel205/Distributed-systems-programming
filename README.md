# Cloud-Based Distributed Projects

This repository contains the implementation of three distributed computing projects, utilizing AWS and MapReduce patterns. The projects involve cloud-based processing of PDF files, generating a knowledge base for a Hebrew word-prediction system, and performing semantic similarity classification using various measures.

## Projects Overview

### 1. **PDF Document Conversion in the Cloud**
This project involves building a cloud-based application that converts PDF files into various formats (image, HTML, or text). It utilizes AWS EC2, S3, and SQS to handle the processing of PDF files in a distributed manner. The user uploads a list of PDF URLs and specifies the operation to be performed on the files. The system then processes the files and returns an HTML file containing the results.

#### Main Tasks:
- Distribute PDF conversion tasks across multiple workers in the cloud using AWS EC2.
- Convert PDF files to either PNG, HTML, or Text formats based on the user input.
- Return the results as a summary HTML file with links to the output files.
- Handle exceptions and errors during processing, and report them in the output.

#### Technologies Used:
- **AWS EC2, S3, SQS** for cloud resources and message queues.
- **Python** for the local application and AWS communication.
- **HTML** for result display.

#### Setup:
1. Set up AWS EC2 instances for the manager and workers.
2. Upload input files to S3.
3. Start the local application to trigger the processing.
4. View the results in an HTML file.

---

### 2. **Hebrew Word-Prediction System Using MapReduce**
This project focuses on building a knowledge base for Hebrew word-prediction based on the Google 3-Gram Hebrew dataset. The task is to calculate the conditional probabilities for word trigrams using the MapReduce framework, implemented on AWS EMR. The project handles large datasets and removes stop words to improve the quality of the knowledge base.

#### Main Tasks:
- Implement a MapReduce job to calculate conditional probabilities for word trigrams.
- Remove trigrams containing stop words.
- Handle large-scale data using scalable techniques.
- Output the trigrams sorted by the probability of the third word.

#### Technologies Used:
- **AWS EMR** for distributed processing.
- **MapReduce** pattern for data computation.
- **Python** for data processing.

#### Setup:
1. Set up an AWS EMR cluster.
2. Use the Hebrew 3-Gram dataset stored on S3.
3. Run the MapReduce job to calculate probabilities.
4. Analyze and view the output trigrams.

---

### 3. **Semantic Similarity - Classification Using Various Measures**
This project involves implementing and modifying an algorithm for semantic similarity based on the research paper *Comparing Measures of Semantic Similarity* by Ljubešić et al. The goal is to modify the algorithm to use the MapReduce pattern for parallelization and then classify semantic similarity using various measures. The results are evaluated by running experiments and analyzing the output.

#### Main Tasks:
- Implement the algorithm from the paper using MapReduce to parallelize the process.
- Modify the algorithm to handle large datasets efficiently.
- Run experiments as described in the paper and analyze the results.
- Report on the quality of the semantic similarity classifications.

#### Technologies Used:
- **MapReduce** for parallel processing.
- **Java** for algorithm implementation and analysis.

#### Setup:
1. Read and understand the research paper on semantic similarity.
2. Modify the provided algorithm to implement MapReduce.
3. Run experiments and classify semantic similarity.
4. Analyze and report results based on the experiments.

---

## General Setup

### Requirements
- **AWS Account**: Required to use AWS EC2, S3, and EMR for distributed processing.
- **MapReduce Framework**: Used for parallelization in the Hebrew Word-Prediction and Semantic Similarity projects.
- **Libraries**: Additional Python libraries might be required depending on the specific project, such as `boto3` for AWS interactions.

### Instructions
1. Clone the repository to your local machine:
   ```bash
   git clone https://github.com/yourusername/repository-name.git
