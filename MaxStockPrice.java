import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxStockPrice {

    public static class MaxStockPriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text stock = new Text();
        private final DoubleWritable price = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            System.out.println(line);
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
            String[] values = new String[tokenizer.countTokens()];
            int i = 0;
            while (tokenizer.hasMoreTokens()) {
                values[i] = tokenizer.nextToken();
                i++;
            }
            if (key.get() > 0 && values.length >= 4) {
                stock.set(((FileSplit) context.getInputSplit()).getPath().getName());
                System.out.println(values);
                price.set(Double.parseDouble(values[4]));
                context.write(stock, price);
            }
        }
    }

    public static class MaxStockPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double maxPrice = Double.NEGATIVE_INFINITY;
            for (DoubleWritable value : values) {
                if (value.get() > maxPrice) {
                    maxPrice = value.get();
                }
            }
            context.write(key, new DoubleWritable(maxPrice));
        }

    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MaxStockPrice");
        job.setJarByClass(MaxStockPrice.class);
        job.setMapperClass(MaxStockPriceMapper.class);
        job.setReducerClass(MaxStockPriceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timestamp = dateFormat.format(new Date());
        System.out.println(timestamp + ": Execution time: " + executionTime + "ms");

        if (result) {
            Path outputFile = new Path(outputPath, "part-r-00000");
            Path resultFile = new Path(args[2]);
            if (fs.exists(resultFile)) {
                fs.delete(resultFile, true);
            }
            fs.copyToLocalFile(outputFile, resultFile);
        }
    }
}

//by Rithik Reddy Matta , Nithin Yadav ,Jatin Sutrave,Sathvik Reddy Chadupatla