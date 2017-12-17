import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.*;

public class equijoin
{
	public static String key1 = "";
	public static String key2 = "";
	static List<String> check = new ArrayList<String>();
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	     
		private Text joinCol = new Text();
	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	    	
	    	String line[] = value.toString().split(",");
	    	//assuming there are going to be only two keys - R and S
	    	if (value.toString() != null) {
				if (key1.isEmpty())
					key1 = line[0];
				else {
					if (key2.isEmpty()) 
						key2 = line[0];					
				}
			}
	    	joinCol.set(line[1]);	    		        	      
	    	output.collect(joinCol, new Text(value.toString()));	    	
	     }
	   }

	   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		   public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
	           
			   List<String> list1 = new ArrayList<String>();
			   List<String> list2 = new ArrayList<String>();			   
			   Text result = new Text();
			   String merge = "";
			   //if there is only a single entry in the block...ignore it. More than 1 entry...add it to the respective list
			   while(values.hasNext())
			   { 
				   String rowVal = values.next().toString();
				   String Split[] = rowVal.split(",");
				   
				   if(Split[0].equals(key1) && !check.contains(rowVal)){
					   list1.add(rowVal);
					   check.add(rowVal);
				   }
				   else if(Split[0].equals(key2)&& !check.contains(rowVal)){
					   list2.add(rowVal);
					   check.add(rowVal);
				   }
			   }
			   for(int i=0; i<list1.size(); i++){
				   for(int j=0;j<list2.size();j++){ 
					   merge= list1.get(i) + "," + list2.get(j);
					   result.set(merge);
					   
					   output.collect(new Text(""), result);	        		
				   }
			   }	         	         	       	        	        	         	        
		   }
	   }

	   public static void main(String[] args) throws Exception {	       	 
	       
		   JobConf job = new JobConf(equijoin.class);
		   job.setJobName("EquiJoin");
		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(Text.class);
		   
		   job.set("mapred.textoutputformat.separator"," ");
		   job.setMapperClass(Map.class);
		   job.setReducerClass(Reduce.class);
		   FileInputFormat.setInputPaths(job,new Path(args[0]));	     
		   FileOutputFormat.setOutputPath(job,new Path(args[1]));
		   
		   
		   JobClient.runJob(job);
	   }
}