package hk.ust.csit5970;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Compute the bigram count using "pairs" approach
 */
public class CORStripes extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(CORStripes.class);

    /*
     * TODO: write your first-pass Mapper here.
     */
    private static class CORMapper1 extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> word_set = new HashMap<String, Integer>();
            // Please use this data structure to keep track of the words in a line
            // Reuse objects
            Text word = new Text();
            IntWritable one = new IntWritable(1);
            String line = ((Text) value).toString();
            String[] words = line.trim().split("\\s+");
            /*
             * TODO: Your implementation goes here.
             */
            for (String w : words) {
                String cleanWord = w.replaceAll("[^a-zA-Z]", "").toLowerCase().trim();
                if (cleanWord.isEmpty()) {
                    continue;
                }
                if (!word_set.containsKey(cleanWord)) {
                    word_set.put(cleanWord, 1);
                    word.set(cleanWord);
                    context.write(word, one);
                }
            }
        }
    }

    /*
     * TODO: write your first-pass Reducer here.
     */
    private static class CORReducer1 extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            /*
             * TODO: Your implementation goes here.
             */
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /*
     * TODO: write your second-pass Mapper here.
     */
    private static class CORStripesMapper2 extends
            Mapper<LongWritable, Text, Text, MapWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            TreeSet<String> word_set = new TreeSet<String>();
            // Please use this data structure to keep track of the words in a line
            // Reuse objects
            MapWritable map = new MapWritable();
            Text word = new Text();
            IntWritable one = new IntWritable(1);
            String line = ((Text) value).toString();
            String[] words = line.trim().split("\\s+");
            /*
             * TODO: Your implementation goes here.
             */
            for (String w : words) {
                String cleanWord = w.replaceAll("[^a-zA-Z]", "").toLowerCase().trim();
                if (cleanWord.isEmpty()) {
                    continue;
                }
                word_set.add(cleanWord);
            }
            for (String w1 : word_set) {
                word.set(w1);
                map.clear();
                for (String w2 : word_set) {
                    if (w2.compareTo(w1) > 0) { // Ensure w2 is alphabetically greater
                        map.put(new Text(w2), one);
                    }
                }
                context.write(word, map);
            }
        }
    }

    /*
     * TODO: write your second-pass Combiner here.
     */
    private static class CORStripesCombiner2 extends
            Reducer<Text, MapWritable, Text, MapWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values,
                Context context) throws IOException, InterruptedException {
            /*
             * TODO: Your implementation goes here.
             */
            MapWritable combinedMap = new MapWritable();
            for (MapWritable map : values) {
                for (Writable otherWord : map.keySet()) {
                    Writable one = (Writable) map.get(otherWord);
                    if (combinedMap.containsKey(otherWord)) {
                        IntWritable prevCount = (IntWritable) combinedMap.get(otherWord);
                        combinedMap.put(otherWord, new IntWritable(prevCount.get() + ((IntWritable)one).get()));
                    } else {
                        combinedMap.put(otherWord, one);
                    }
                }
            }
            context.write(key, combinedMap);
        }
    }

    /*
     * TODO: write your second-pass Reducer here.
     */
    private static class CORStripesReducer2 extends
            Reducer<Text, MapWritable, PairOfStrings, DoubleWritable> {

        private HashMap<String, Integer> word_total_map = new HashMap<String, Integer>();
        // Please use this data structure to keep track of the total number of
        // occurrences of each word

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path middle_result = new Path("mid/part-r-00000"); // Fixed path
            FSDataInputStream in = fs.open(middle_result);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                word_total_map.put(parts[0], Integer.parseInt(parts[1]));
            }
            br.close();
            in.close();
            /*
             * TODO: Your setup implementation goes here.
             */
        }

        @Override
        public void reduce(Text key, Iterable<MapWritable> values,
                Context context) throws IOException, InterruptedException {
            /*
             * TODO: Your implementation goes here.
             */
            MapWritable combinedMap = new MapWritable();
            for (MapWritable map : values) {
                for (Writable otherWord : map.keySet()) {
                    Writable one = (Writable) map.get(otherWord);
                    if (combinedMap.containsKey(otherWord)) {
                        IntWritable prevCount = (IntWritable) combinedMap.get(otherWord);
                        combinedMap.put(otherWord, new IntWritable(prevCount.get() + ((IntWritable)one).get()));
                    } else {
                        combinedMap.put(otherWord, one);
                    }
                }
            }

            PairOfStrings pair = new PairOfStrings();
            DoubleWritable cor = new DoubleWritable();
            for (Writable otherWord : combinedMap.keySet()) {
                String w1 = key.toString();
                String w2 = otherWord.toString();
                int sum = ((IntWritable) combinedMap.get(otherWord)).get();
                double frequencyW1 = word_total_map.get(w1);
                double frequencyW2 = word_total_map.get(w2);
                pair.set(w1, w2);
                cor.set((double) sum / (frequencyW1 * frequencyW2));
                context.write(pair, cor);
            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public CORStripes() {
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));
        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: "
                    + exp.getMessage());
            return -1;
        }

        // Lack of arguments
        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
                .parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;
        LOG.info("Tool: " + CORStripes.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        // Setup for the first-pass MapReduce
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Firstpass");

        job1.setJarByClass(CORStripes.class);
        job1.setMapperClass(CORMapper1.class);
        job1.setReducerClass(CORReducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path("mid")); // Temporary output

        // Delete the output directory if it exists already.
        Path middleDir = new Path("mid");
        FileSystem.get(conf1).delete(middleDir, true);

        // Time the program
        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        LOG.info("Job 1 Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");

        // Setup for the second-pass MapReduce

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf1).delete(outputDir, true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Secondpass");

        job2.setJarByClass(CORStripes.class);
        job2.setMapperClass(CORStripesMapper2.class);
        job2.setCombinerClass(CORStripesCombiner2.class);
        job2.setReducerClass(CORStripesReducer2.class);

        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(MapWritable.class);
        job2.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        // Time the program
        startTime = System.currentTimeMillis();
        job2.waitForCompletion(true);
        LOG.info("Job 2 Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CORStripes(), args);
    }
}
