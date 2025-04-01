package hk.ust.csit5970;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class BigramFrequencyStripes extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BigramFrequencyStripes.class);

    private static class MyMapper extends Mapper<LongWritable, Text, Text, HashMapStringIntWritable> {
        private static final Text KEY = new Text();
        private static final HashMapStringIntWritable STRIPE = new HashMapStringIntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.trim().split("\\s+");
            for (int i = 0; i < words.length - 1; i++) {
                String word1 = words[i];
                String word2 = words[i + 1];
                KEY.set(word1);
                STRIPE.clear();
                STRIPE.increment(word2);
                context.write(KEY, STRIPE);
            }
        }
    }

    private static class MyReducer extends Reducer<Text, HashMapStringIntWritable, PairOfStrings, FloatWritable> {
        private final static PairOfStrings BIGRAM = new PairOfStrings();
        private final static FloatWritable FREQ = new FloatWritable();

        private Map<String, Integer> marginalCounts = new HashMap<>();
        private Map<PairOfStrings, Integer> jointCounts = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<HashMapStringIntWritable> stripes, Context context) throws IOException, InterruptedException {
            HashMapStringIntWritable sumStripes = new HashMapStringIntWritable();
            for (HashMapStringIntWritable stripe : stripes) {
                sumStripes.plus(stripe);
            }

            int totalCount = 0;
            for (int count : sumStripes.values()) {
                totalCount += count;
            }

            marginalCounts.put(key.toString(), totalCount);

            for (Map.Entry<String, Integer> entry : sumStripes.entrySet()) {
                String word2 = entry.getKey();
                int count = entry.getValue();
                BIGRAM.set(key.toString(), word2);
                jointCounts.put(BIGRAM, jointCounts.getOrDefault(BIGRAM, 0) + count);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // First output the marginal counts for each word
            for (Map.Entry<String, Integer> entry : marginalCounts.entrySet()) {
                String word = entry.getKey();
                int count = entry.getValue();
                BIGRAM.set(word, "");
                FREQ.set(count);
                context.write(BIGRAM, FREQ);
            }

            // Then output the relative frequencies for each bigram
            for (Map.Entry<PairOfStrings, Integer> entry : jointCounts.entrySet()) {
                PairOfStrings bigram = entry.getKey();
                int jointCount = entry.getValue();
                String leftWord = bigram.getLeftElement();

                float relativeFrequency = (float) jointCount / marginalCounts.get(leftWord);
                FREQ.set(relativeFrequency);
                context.write(bigram, FREQ);
            }
        }
    }

    private static class MyCombiner extends Reducer<Text, HashMapStringIntWritable, Text, HashMapStringIntWritable> {
        private final static HashMapStringIntWritable SUM_STRIPES = new HashMapStringIntWritable();

        @Override
        public void reduce(Text key, Iterable<HashMapStringIntWritable> stripes, Context context) throws IOException, InterruptedException {
            SUM_STRIPES.clear();
            for (HashMapStringIntWritable stripe : stripes) {
                SUM_STRIPES.plus(stripe);
            }
            context.write(key, SUM_STRIPES);
        }
    }

    public BigramFrequencyStripes() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("number of reducers").create(NUM_REDUCERS));
        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

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
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;
        LOG.info("Tool: " + BigramFrequencyStripes.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(BigramFrequencyStripes.class.getSimpleName());
        job.setJarByClass(BigramFrequencyStripes.class);

        job.setNumReduceTasks(reduceTasks);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HashMapStringIntWritable.class);
        job.setOutputKeyClass(PairOfStrings.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);

        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BigramFrequencyStripes(), args);
    }
}
