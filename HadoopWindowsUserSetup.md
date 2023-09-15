*Apache Spark doesn't have its system to organize files in a distributed way (the file system) therefore, it requires 
any file systems to store and process large datasets. For this reason, programmers install Spark 
on top of Hadoop so that Spark's advanced analytics applications can make use of the data stored using the Hadoop Distributed 
File System(HDFS).*

****Prerequisites:****

Before you start installing Hadoop on Windows, there are a few prerequisites that you need to have in place:

1. Java Development Kit (JDK) version 8 or higher
2. Apache Hadoop distribution suitable for Windows

**Step 1:**  *Install the Java Development Kit* 
`Hadoop is built using Java, so you’ll need to install the Java Development Kit (JDK) version 8 
or higher on your computer. You can download the JDK from the Oracle website.
(https://www.oracle.com/in/java/technologies/javase/javase8-archive-downloads.html) Once the download is 
complete, run the installer and follow the instructions to install the JDK.`

**Step 2:** *Download the Hadoop distribution*
`To install Hadoop on Windows, you’ll need to download the appropriate distribution from the 
Apache Hadoop website (https://hadoop.apache.org/releases.html). 
You’ll want to choose the distribution that is compatible with your version of Windows (hadoop-3.3.6) and click on binary. 
Once you’ve downloaded the distribution, extract files of hadoop-3.3.6.tar.gz and place under “C:\Hadoop”.`

**Step 3:** *Set up the Environment Variables*

To use Java & Hadoop, you’ll need to set up some environment variables.
This will allow you to run Java & Hadoop commands from any directory on your computer. 
To set up the environment variables, follow these steps:

1. Open the Start menu and search for “Environment Variables”.
2. Click on “Edit the system environment variables”.
3. Click on the “Environment Variables” button.
4. Under “System Variables”, click on “New”.
5. Enter “JAVA_HOME” as the variable name & the path to the directory where your java is installed (example- C:\Program Files\Java\jdk1.8.0) as the variable value.
6. Click “OK”.
7. Enter “HADOOP_HOME” as the variable name and the path to the directory where you extracted the Hadoop distribution (example- C:\hadoop) as the variable value.
8. Click “OK”.
9. Locate the “Path” variable in the “System Variables” list and click “Edit”.
10. Add the following to the end of the “Variable value” field: %JAVA_HOME%\bin; %HADOOP_HOME%\bin; %HADOOP_HOME%\sbin;
11. Click “OK” to close all the windows.

**Step 4:** *Install Hadoop native IO binary*
`Clone or download the winutils repository (https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin) 
and copy the contents of hadoop-3.3.5/bin into the extracted location of the Hadoop binary package. 
In our example, it will be C:\hadoop\bin.`


**Important Note:** Below steps are not necessary for Spark to run, above steps are sufficient to work with Spark. 
However, you can proceed if you really want the entire Hadoop distribution to work with.


**Step 5:** *Hadoop Configuration*
To configure Hadoop, you’ll need to modify a few configuration files. 
These files are located in the “etc/hadoop” directory of the Hadoop folder. 
Open each of the following files in a text editor and make the changes described below and save the files:

1. `core-site.xml`: Add the following lines to the file inside `<configuration>` like this:
```
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

2. Open this file `hadoop-env.cmd` (windows command script) and replace `set JAVA_HOME=%JAVA_HOME%`
with java installed location like this `set JAVA_HOME=C:\Program Files\Java\jdk1.8.0` 
or if it doesn't work then use this `set JAVA_HOME=C:\Progra~1\Java\jdk1.8.0` also go to bottom of the file
and give your name to this variable like this : `set HADOOP_IDENT_STRING=RockTheJVM`.


3. `hdfs-site.xml`: First create these folders - `C:/hadoop/data/dfs/datanode` and `C:/hadoop/data/dfs/datanode`
Add the following lines to the file inside `<configuration>` like this:
```
<property>
<name>dfs.replication</name>
<value>1</value>
</property>
<property>
<name>dfs.namenode.name.dir</name>
<value>file:///C:/hadoop/data/dfs/namenode</value>
</property>
<property>
<name>dfs.datanode.data.dir</name>
<value>file:///C:/hadoop/data/dfs/datanode</value>
</property>
```

4. `mapred-site.xml`: Add the following lines to the file inside `<configuration>` like this:
```
<configuration>
   <property>
       <name>mapreduce.framework.name</name>
       <value>yarn</value>
   </property>
</configuration>
```

5. `yarn-site.xml`: Add the following lines to the file inside `<configuration>` like this:
```
<configuration>
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
<description>Yarn Node Manager Aux Service</description>
</property>
<configuration>
```

**Step 6:** **If you want to start Hadoop:**
To start Hadoop, open a command prompt and navigate to the directory where you extracted the Hadoop distribution. 
Then, run the following commands:
```
cd sbin
start-all.cmd
```
This will start the Hadoop daemons and launch the web interface. You can access the web interface by going to http://localhost:9000/ in your web browser.

****Conclusion:****
Setting up Hadoop on a Windows system can pose some challenges, but by following this comprehensive guide, 
you'll be able to configure it smoothly and quickly. Hadoop is a robust solution for handling extensive datasets and executing distributed applications, 
making it a favored choice for numerous enterprises and institutions worldwide. 
Whether you're a data scientist or a software developer, integrating Hadoop into your toolkit is highly beneficial.
















