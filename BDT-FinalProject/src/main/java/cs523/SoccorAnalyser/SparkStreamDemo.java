package cs523.SoccorAnalyser;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import org.apache.log4j.*;
import org.apache.spark.streaming.api.java.*;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SparkStreamDemo
{

	public static void main(String[] args) throws Exception
	{
		//local[*] represents that use all avialable core
		//in local mode, thread represents all the cores
		SparkConf conf = new SparkConf().setAppName("CS523-SoccorAnalysis-FinalProject").setMaster("local[*]");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(20));
		//Logger.getRootLogger().setLevel(Level.ERROR);
		
		JavaDStream<String> lines = streamingContext.textFileStream("/home/cloudera/workspace/BDT-FinalProject/input/");
		
		// Define the path to the text file where you want to append the data (used for logging purpose)
        String outputPath = "/home/cloudera/workspace/BDT-FinalProject/output/output.txt"; 
        
     // Define the path to the text file where you want to append the data (used for goal data)
        String outputPathGoal = "/home/cloudera/workspace/BDT-FinalProject/output/outputGoal.txt"; 
        
     // Define the path to the text file where you want to append the data (used for organizerCity)
        String outputPathOrganizerCity = "/home/cloudera/workspace/BDT-FinalProject/output/outputCity.txt"; 
        
     // Define the path to the text file where you want to append the data (used for goal data)
        String outputPathMatchType = "/home/cloudera/workspace/BDT-FinalProject/output/outputMatchTtype.txt"; 
        
		
		// Use the transform operation to exclude the first line
        JavaDStream<String> filteredLines = lines.transform(rdd -> rdd.zipWithIndex().filter(tuple -> tuple._2() > 0).map(tuple -> tuple._1()));
        
        String[] FromToDate = new String[2];
        
        // Use foreachRDD to collect the first and last data
        filteredLines.foreachRDD(rdd -> {
            // Collect the first and last elements of the RDD
            if (!rdd.isEmpty()) {
                String firstElement = rdd.first();
                String lastElement = rdd.reduce((a, b) -> b);
                String[] partsFirst = firstElement.split(",");
                String[] partsLast = lastElement.split(",");
                FromToDate[0]=partsFirst[0];
                FromToDate[1]=partsLast[0];                
                // You can perform any action with the first and last elements here
                System.out.println("Dataset Tournament Start Date: " + FromToDate[0]);
                System.out.println("Dataset Tournament End Date: " + FromToDate[1]);
                try (FileWriter writer = new FileWriter(outputPath, true)) {
                	// Iterate over the elements of the string array
                    for (int i=0; i<FromToDate.length;i++) {
                    	if(i==0){
                        // Write each element to the file, followed by a newline character
                        writer.write("Dataset Tournament Start Date" +FromToDate[i] + "\n");
                    	}
                    	else{
                    		 writer.write("Dataset Tournament End Date" +FromToDate[i] + "\n");
                    	}
                    }
                } 
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
     

        //Case1: calculating total score based on home and away goals and filtering country who scored more than 50 goals
        
        //calculating home-team 
        // Split each line into two columns: home_team and home_score
        JavaPairDStream<String, Integer> homeScores = filteredLines.mapToPair(line -> {
            String[] parts = line.split(",");
            if (parts.length == 9) {
                String homeTeam = parts[1].trim();
                String strNumber = parts[3].replaceAll("^\"|\"$", "");
                int homeScore = Integer.parseInt(strNumber.trim());
                return new Tuple2<>(homeTeam, homeScore);
            } else {
                // Handle invalid lines or missing data
                return new Tuple2<>("Invalid", 0);
            }
        });
        
      //calculating away-team 
        // Split each line into two columns: home_team and home_score
        JavaPairDStream<String, Integer> awayScores = filteredLines.mapToPair(line -> {
            String[] parts = line.split(",");
            if (parts.length == 9) {
                String awayTeam = parts[2].trim();
                String strNumber = parts[4].replaceAll("^\"|\"$", "");
                int awayScore = Integer.parseInt(strNumber.trim());
                return new Tuple2<>(awayTeam, awayScore);
            } else {
                // Handle invalid lines or missing data
                return new Tuple2<>("Invalid", 0);
            }
        });
        
     // Combine the homeScores and awayScores using union
        JavaPairDStream<String, Integer> combinedScores = homeScores.union(awayScores);

        // Reduce the scores by key
        JavaPairDStream<String, Integer> totalScores = combinedScores.reduceByKey(Integer::sum);
        
     // Filter the totalScores to keep only values above 50
        JavaPairDStream<String, Integer> scoresAbove50 = totalScores.filter(tuple -> tuple._2() > 50);
        
     // Sort the scores by value in descending order
        JavaPairDStream<Integer, String> sortedScores = scoresAbove50.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                .transformToPair(rdd ->
                 rdd.sortByKey(false)
                );
        
        //printed totalsorted score in console
        sortedScores.print();
        
     // Using foreachRDD to write each line to a file using FileWriter
        sortedScores.foreachRDD(rdd -> {
            rdd.foreachPartition(iter -> {
                try (FileWriter writer = new FileWriter(outputPath, true)) {
                	boolean partitionContainsData = iter.hasNext();
                	if(partitionContainsData){
                	writer.write("******Writing total goal scored by each country********\n");
                	writer.write("The format is <Total Number of goals, Country> \n");
                	}
                    while (iter.hasNext()) {
                        Tuple2<Integer, String> record = iter.next();
                        writer.write(record._1() + "," + record._2() + "\n");
                        //saveToHBase(FromToDate[0],FromToDate[1], record._1().toString() ,record._2());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });
        
     // Using foreachRDD to write each line to a file using FileWriter (this one will be the input for hive)
        sortedScores.foreachRDD(rdd -> {
            rdd.foreachPartition(iter -> {
                try (FileWriter writer = new FileWriter(outputPathGoal, true)) {
                	boolean partitionContainsData = iter.hasNext();
                	if(partitionContainsData){
                	}
                    while (iter.hasNext()) {
                        Tuple2<Integer, String> record = iter.next();
                        writer.write(FromToDate[0]+ "," +FromToDate[1]+ "," + record._1() + "," + record._2() + "\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });
      //case 2: calculating the type of the match and order it in Descending order
      //to find the popularity of different type of matches at different timeframe
        JavaPairDStream<String, Integer> matchType = filteredLines.mapToPair(line -> {
            String[] parts = line.split(",");
            if (parts.length == 9) {
                String matchClass = parts[5].trim();
                return new Tuple2<>(matchClass, 1);
            } else {
                // Handle invalid lines or missing data
                return new Tuple2<>("Invalid", 0);
            }
        });
        


        // Reduce the match type by number of matches
        JavaPairDStream<String, Integer> totalMatchType = matchType.reduceByKey(Integer::sum);
        
     // Sort the match type by value in descending order
        JavaPairDStream<Integer, String> sortedMatchType = totalMatchType.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                .transformToPair(rdd ->
                 rdd.sortByKey(false)
                );
        
        //print sorted match type in console
        sortedMatchType.print();
        
     // Using foreachRDD to write each line to a file using FileWriter
        sortedMatchType.foreachRDD(rdd -> {
            rdd.foreachPartition(iter -> {
                try (FileWriter writer = new FileWriter(outputPath, true)) {
                	boolean partitionContainsData = iter.hasNext();
                	if(partitionContainsData){
                	writer.write("******Writing type of match and total number of match********\n");
                	writer.write("The format is <Total Number of match, MatchType> \n");
                	}
                    while (iter.hasNext()) {
                        Tuple2<Integer, String> record = iter.next();
                        writer.write(record._1() + "," + record._2() + "\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });
        
     // Using foreachRDD to write each line to a file using FileWriter (this one will be the input for hive)
        sortedMatchType.foreachRDD(rdd -> {
            rdd.foreachPartition(iter -> {
                try (FileWriter writer = new FileWriter(outputPathMatchType, true)) {
                	boolean partitionContainsData = iter.hasNext();
                	if(partitionContainsData){
                	}
                    while (iter.hasNext()) {
                        Tuple2<Integer, String> record = iter.next();
                        writer.write(FromToDate[0]+ "," +FromToDate[1]+ "," + record._1() + "," + record._2() + "\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });
        
        //Case 3: Find the pattern of International football tournament organizer by city with threshold 40     
        JavaPairDStream<Tuple2<String, String>, Integer> organizerCity = filteredLines.mapToPair(line -> {
            String[] parts = line.split(",");             
            String city = parts[6]; // First part as the first key
            String country = parts[7]; // Second part as the second key
            Tuple2<Tuple2<String, String>, Integer> tupleKey = new Tuple2<>(new Tuple2<>(city, country), 1);
            return tupleKey;
        });
        


        // Reduce the match type by number of matches
        JavaPairDStream<Tuple2<String, String>, Integer> totalOrganizerCity = organizerCity.reduceByKey(Integer::sum);
        
     // Filter the city to keep only values above 40
        JavaPairDStream<Tuple2<String, String>, Integer> organizerCityAbove40 = totalOrganizerCity.filter(tuple -> tuple._2() > 50);
        
        //printer organizer city above 40 in given timeframe in console
        organizerCityAbove40.print();
        
     // Use foreachRDD to write the values to the output file
        organizerCityAbove40.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                try (FileWriter writer = new FileWriter(outputPath, true)) {
                	boolean partitionContainsData = partition.hasNext();
                	if(partitionContainsData){
                	writer.write("******Writing pattern of International football tournament organizer by city for threshold 40********\n");
                	writer.write("The format is <<City, Country>, TotalNumberofMatches> \n");
                	}
                    while (partition.hasNext()) {
                        Tuple2<Tuple2<String, String>, Integer> data = partition.next();
                        writer.write(data._1() + "," +data._2() + "\n"); // Write the value (integer) to the file
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });
        
     // Use foreachRDD to write the values to the output file (this one will be the input for the hive)
        organizerCityAbove40.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                try (FileWriter writer = new FileWriter(outputPathOrganizerCity, true)) {
                	boolean partitionContainsData = partition.hasNext();
                	if(partitionContainsData){
                	}
                    while (partition.hasNext()) {
                        Tuple2<Tuple2<String, String>, Integer> data = partition.next();
                        writer.write(FromToDate[0]+ "," +FromToDate[1]+ "," + data._1() + "," + data._2() + "\n"); // Write the value (integer) to the file
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });
        
        //try catch is implemented to catch unexpected error handling        
        try {
      	  // streaming start and await termination
      		streamingContext.start();
      		streamingContext.awaitTermination();
      	} 
      catch(Exception e)
      	{
      	 
      	    // Handle the exception gracefully (e.g., log it)
      		System.out.println("An error occurred: ${e.getMessage}");
      	} 
      finally {
      	  	//perform cleanup or finalization tasks 
      		streamingContext.stop();
      	}
        
	}
	
	 private static void saveToHBase(String fromDate, String toDate, String score, String Country) {
	        try {
	            Configuration conf = HBaseConfiguration.create();
	            conf.set("hbase.zookeeper.quorum", "localhost");
	            conf.set("hbase.zookeeper.property.clientPort", "2183");
	            conf.set("hbase.master", "localhost:16010");

	            Connection connection = ConnectionFactory.createConnection(conf);
	            Admin admin = connection.getAdmin();
	            System.out.println("Table created");
	            // Define the table name and column family
	            TableName tableName = TableName.valueOf("SoccorScoreTable");

	            Table table = connection.getTable(tableName);

	            // Create a Put object to specify the row key
	            Put put = new Put(Bytes.toBytes("row1")); // Change "row1" to your desired row key
	            Put soccorData = new Put(Bytes.toBytes("row1"));

	            // Add data to the Put object
	            soccorData.addColumn(Bytes.toBytes("fromDate"), Bytes.toBytes("From"), Bytes.toBytes(fromDate));
	            soccorData.addColumn(Bytes.toBytes("toDate"), Bytes.toBytes("To"), Bytes.toBytes(toDate));
	            soccorData.addColumn(Bytes.toBytes("Score"), Bytes.toBytes("TotalScore"), Bytes.toBytes(score));
	            soccorData.addColumn(Bytes.toBytes("Country"), Bytes.toBytes("CountryName"), Bytes.toBytes(Country));
	            table.put(put);
	            // Close the table when done
	            table.close();
	            System.out.println("Table created successfully.");

	            admin.close();
	            connection.close();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }
}
