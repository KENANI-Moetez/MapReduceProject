package FastestLapAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FastestLapMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text driverName = new Text();
    private IntWritable fastestLapCount = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        // Assuming the driver's name is in the first column
        driverName.set(columns[0]);

        context.write(driverName, fastestLapCount);
    }
}
