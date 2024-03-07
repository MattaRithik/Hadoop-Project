# Hadoop-Project

Problem Statement :
Write a MapReduce program in Hadoop to find the Max price of stock_price_high for each stock. Capture the running time programmatically.


Requirements:
Multinode Hadoop_3.2.1
Java jdk Latest Version 

Steps to execute:

Step 1:

//Create a new folder inside hdfs

start-dfs.sh
start-yarn.sh
hdfs dfs -mkdir /ADVANCENYSE

Step 2:
// Put the input file into the hdfs folder 
 
 hdfs dfs -put /Path_of_the_input /ADVANCENYSE/
 
 Step 3:
//initialize the hadoop_classpath

 export HADOOP_CLASSPATH=$(hadoop classpath)
 echo $HADOOP_CLASSPATH
 
 Step 3:
//Now Convert the Java File which is the folder into javac(Compiled) file using javac 
 
 javac -classpath $(HADOOP_CLASSPATH) -d "Path_of_the_folder_to_store_the_ compiled_classes_of_the_java_file' 'Path_of_the_Java_file'
 
 Step 4:
 //Now we need to convert the java files into the jar files to implement on the data set 
 
 jar -cvf NameOfJarFile.jar -C Folder_Name_of_Compiled_Classes/ .
 
 Step 5:
 // Now we need to run the Map Reduce Function on the dataset that is in the HDFS dataset 
 
hadoop jar 'Path_of_jar_File' Class_Name  /Input_folder_Path /Output_Folder_Path

Step 6:
//To view the ouput
hdfs dfs -cat 'path_Name_of_Output_folder' 

Step 7 :
//To download the output into local machine by using Localhost method 
http://master:9870/explorer.html#/AdvanceNYSE
