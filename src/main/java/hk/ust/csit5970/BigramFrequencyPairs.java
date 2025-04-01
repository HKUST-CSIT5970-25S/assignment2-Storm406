package hk.ust.csit5970;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Compute the bigram relative frequency using "pairs" approach
 */
public class BigramFrequencyPairs extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BigramFrequencyPairs.class);

    /*
     * Mapper emits each bigram with count 1
     */
    private static class MyMapper extends
            Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

        // Reuse objects to save overhead of object creation.
        private static final IntWritable ONE = new IntWritable(1);
        private static final PairOfStrings BIGRAM = new PairOfStrings();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = ((Text) value).toString();
            String[] words = line.trim().split("\\s+");
            
            for (int i = 0; i < words.length - 1; i++) {
                String word1 = words[i];
                String word2 = words[i + 1];
                BIGRAM.set(word1, word2);
                context.write(BIGRAM, ONE);
            }
        }
    }

    /*
     * Reducer calculates the relative frequency for each bigram
     */
    private static class MyReducer extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, FloatWritable> {

        // Reuse objects.
        private final static FloatWritable VALUE = new FloatWritable();
        private final static PairOfStrings WORD_PAIR = new PairOfStrings();
        
        private Map<String, Integer> marginalCounts = new HashMap<>();
        private Map<PairOfStrings, Integer> jointCounts = new HashMap<>();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            // Get the first word of the bigram
            String leftWord = key.getLeftElement();
            
            // Count occurrences of this bigram
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            
            // Store the count for this specific bigram
            PairOfStrings bigram = new PairOfStrings(leftWord, key.getRightElement());
            jointCounts.put(bigram, sum);
            
            // Update the marginal count for the left word
            if (marginalCounts.containsKey(leftWord)) {
                marginalCounts.put(leftWord, marginalCounts.get(leftWord) + sum);
            } else {
                marginalCounts.put(leftWord, sum);
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // First output the marginal counts for each word
            for (Map.Entry<String, Integer> entry : marginalCounts.entrySet()) {
                String word = entry.getKey();
                int count = entry.getValue();
                
                WORD_PAIR.set(word, "");
                VALUE.set(count);
                context.write(WORD_PAIR, VALUE);
            }
            
            // Then output the relative frequencies for each bigram
            for (Map.Entry<PairOfStrings, Integer> entry : jointCounts.entrySet()) {
                PairOfStrings bigram = entry.getKey();
                int jointCount = entry.getValue();
                String leftWord = bigram.getLeftElement();
                
                // Calculate the relative frequency
                float relativeFrequency = (float) jointCount / marginalCounts.get(leftWord);
                
                VALUE.set(relativeFrequency);
                context.write(bigram, VALUE);
            }
        }
    }

    private static class MyCombiner extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    /*
     * Partition bigrams based on their left elements
     */
    private static class MyPartitioner extends
            Partitioner<PairOfStrings, IntWritable> {
        @Override
        public int getPartition(PairOfStrings key, IntWritable value,
                int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE)
                    % numReduceTasks;
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public BigramFrequencyPairs() {
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
        LOG.info("Tool: " + BigramFrequencyPairs.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        // Create and configure a MapReduce job
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(BigramFrequencyPairs.class.getSimpleName());
        job.setJarByClass(BigramFrequencyPairs.class);

        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairOfStrings.class);
        job.setOutputValueClass(FloatWritable.class);
        
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);
        
        // Time the program
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");
        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BigramFrequencyPairs(), args);
    }
}
