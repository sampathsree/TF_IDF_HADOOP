# TF_IDF_HADOOP
Term Frequency - Inverse Document Frequency is used in Information Retrieval. This is implemented in distributed computing environment using Apache HADOOP.<br/>
<b>Steps to execute:</b><br/>
1. Setup Hadoop input folder with sample input.<br/>
2. Setup build folder to build java files<br/>
3. Following is the command to build the files:<br/>
$ <i>javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build –Xlint</i><br/>
$ <i>javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java -d build –Xlint</i><br/>
$ <i>javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TFIDF.java -d build –Xlint</i><br/>
4. After Executing above commands, class files are generated in the build folder. Now Run the following commands to generate .jar file:<br/>
$ <i>jar -cvf wordcount.jar -C build/ .</i><br/>
-> JAR file can have any name<br/>
5. Once the “.jar” file is created, we can execute the three programs using following commands:<br/>
$ <i>hadoop jar wordcount.jar org.myorg.WordCount input DocWordCount</i><br/>
$ <i>hadoop jar wordcount.jar org.myorg.TermFrequency input TermFrequency</i><br/>
$ <i>hadoop jar wordcount.jar org.myorg.TFIDF input TFIDF</i><br/>
- Here “input” is the input folder path.<br/>
<b>Note:</b><br/>
1. Output folder need not be deleted every time we run the command, as I have written code to delete the output folder if it exists.<br/>
2. TFIDF.java program automatically takes the count of number files present in input folder. No need to modify the code every time we run for multiple files.