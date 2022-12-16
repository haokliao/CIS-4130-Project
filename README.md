# CIS 4130 Semester Project

Our semester long project was to build a machine learning pipeline for our Big Data Technologies class.
This pipeline incorporates cloud infrastructure, in our case utilizing different aspects of AWS such as EC2 instances and EMR clusters.

This project is divided into five different Milestones, all with their own components and requirements!

- Milestone 1: Project Proposal
- Milestone 2: Data Acquisition
- Milestone 3: Descriptive Statistics
- Milestone 4: Coding and Modeling
- Milestone 5: Visualizing Results

### Set up

The majority of the PySpark and Python code, as well as the milestones and descriptions are all located on the `project_final.ipynb` file.

Additionally, you will need to create an EC2 instance and follow instructions located on the `Steps for downloading data directly from Kaggle to AWS.docx` file. 

Once the Kaggle to AWS instruction is followed, and the yelp dataset is downloaded from Kaggle, and unzipped back into the S3 bucket, the next steps are to run an EMR Cluster on a 6.7.0 Version and 5 instances. 

Upon successful creation of this cluster, look for the EC2 instance that was created with the same ipv4 address as your EMR Cluster, and connect with the username hadoop.

Following that, run the code within Milestone 3/4 and see things happen in real time!