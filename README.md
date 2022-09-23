# Hadoop_KNN

### Overview

For this project, I implemented the All kNN algorithm of the paper “Processing All k-Nearest Neighbor Queries in Hadoop”.

### System Manual 

#### Running the system - Command Line 

1. Installing Java / Setting $JAVA_HOME 

  * This system requires Java to run. If you already have Java installed, confirm
that you have version 8. You can check this by running the command “java -version”. Please also set $JAVA_HOME environment variable by typing “JAVA_HOME=/usr/local/jdk1.8.0_101” 

2. Compiling the program with classpath information 

  * Go to the root directory and run the command to compile *.java files into build directory. “javac -cp /opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/h 
adoop-mapreduce/*:src/gson-2.6.2.jar src/*.java -d build -Xlint”. 

3. Creating JAR file 

  * Run the command “jar cvf KNN.jar -C build/ .” to create a jar file. 

4. Running the algorithm 

  * Execute the jar file of the algorithm with the command “hadoop jar KNN.jar KNN [args]”. It requires the following command-line arguments in the following order: 
    * Input HDFS path: the path of the input file (str) 
    * Output HDFS path: the path of the output file (str) 
    * K: the K nearest neighbors (int) 
    * N: the granularity of the decomposition (int) 
    * R: the number of reduce tasks (int) 
