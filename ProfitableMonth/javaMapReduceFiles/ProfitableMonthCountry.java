import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ProfitableMonthCountry {
    // Mapper class
    public static class CountryMapper extends Mapper<Object, Text, Text, Text> {
        private final Text season = new Text();
        private final Text country = new Text();
        // Map method that processes one line at a time, as provided by the specified TextInputFormat
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input record and extract the necessary attributes
            String[] attributes = value.toString().split(",");
            String season_str = attributes[5];
            int bookingStatus = Integer.parseInt(attributes[0]);
            String country_str = attributes[3];
            // Filter out canceled bookings
            if (bookingStatus == 0) {
                // Setting season as key and country as value
                season.set(season_str);
                country.set(country_str);
                context.write(season, country);
            }
        }
    }
    // Reducer class
    public static class TopCountryReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Map<String, String> topCountryMap;
        // Setup method that runs before the task
        protected void setup(Context context) throws IOException, InterruptedException {
            topCountryMap = new HashMap<>();
        }
        // Reduce method that takes the input values, processes them, and outputs the reduced value
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String topCountry = "";
            int maxCount = 0;
            Map<String, Integer> countryCountMap = new HashMap<>();
            // Count the occurrences of each country
            for (Text val : values) {
                String country = val.toString();
                int count = countryCountMap.getOrDefault(country, 0) + 1;
                countryCountMap.put(country, count);
                if (count > maxCount) {
                    topCountry = country;
                    maxCount = count;
                }
            }
            // Update the top country for the season
            topCountryMap.put(key.toString(), topCountry);
        }
        // Cleanup method that is run once after all calls to the reduce method for a particular key
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the top country for each season
            for (Map.Entry<String, String> entry : topCountryMap.entrySet()) {
                String season = entry.getKey();
                String topCountry = entry.getValue();
                context.write(NullWritable.get(), new Text(season + "\t" + topCountry));
            }
        }
    }
    // Driver code - where the job's configurations are set
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Create a Job instance
        Job job = Job.getInstance(conf, "Top Country by Season");
        // Set the Jar by finding where a given class came from
        job.setJarByClass(ProfitableMonthCountry.class);
        // Set the Mapper and Reducer class for the job
        job.setMapperClass(CountryMapper.class);
        job.setReducerClass(TopCountryReducer.class);
        // Set the output key and value types for the entire job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        // Set the input and output file paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Submit the job and wait for it to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

