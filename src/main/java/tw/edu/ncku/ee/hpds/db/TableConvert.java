package tw.edu.ncku.ee.hpds.db;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
// value, value 
// to column='value',column='value'
public class TableConvert{

    public static class TableConvertMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private Text interKey = new Text();
        private Text interValue = new Text();
        private StringBuffer mStrBuf = new StringBuffer();
        private ArrayList<String> mColumns = new ArrayList<String>();
        private ArrayList<String> mPrimaryKeys = new ArrayList<String>();
        private ArrayList<Integer> mPKIdx = new ArrayList<Integer>();

        //----------------------------------------------------------------------------
        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {

            FileSystem dfs = FileSystem.get(context.getConfiguration());
            Path path = new Path(context.getConfiguration().get("schema"));

            FSDataInputStream is = dfs.open(path);


            //Schema=" ";
            //PrimaryKey=" ";
            mColumns.clear();
            mPrimaryKeys.clear();
            mPKIdx.clear();
            String line = "";
            while( (line=is.readLine()) != null ) {
                String[] pairs = line.split("=");
                if ( pairs[0].equals("schema") ) {
                    String[] columns = pairs[1].split(",");
                    for ( String s : columns ) {
                        mColumns.add(s);
                    }
                }
                if ( pairs[0].equals("primary_keys") ) {
                    String[] pkeys = pairs[1].split(",");
                    for ( String s : pkeys ) {
                        mPrimaryKeys.add(s);
                    }
                }
            }
            // construct the primary key idx
            if ( mPrimaryKeys.size() > 0 ) {
                for ( String s : mPrimaryKeys ) {
                    int i = 0;
                    for ( String c : mColumns ) {
                        if ( c.equals(s) ) {
                            mPKIdx.add(new Integer(i));
                            break;
                        }
                        i++;
                    }
                }
            }

            is.close();
        }
        
        private StringBuilder sbStr = new StringBuilder();
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

            String rawString = value.toString();
            //values
            sbStr.setLength(0);
            String[] vals = rawString.split(",");
            
            for ( int i = 0 ; i < mColumns.size() ; i++ ) {
                if ( i != 0 ) {
                    sbStr.append(",");
                }
                sbStr.append(mColumns.get(i)).append("=").append(vals[i]);
            }
            interValue.set(sbStr.toString());
            context.write(NullWritable.get(), interValue);

             
        }// end of map

    }

    public static class TableConvertReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
            /*
            for( Text val : values){
                context.write(key, val);
            }
            */
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: "+ TableConvert.class.getName() +"<csv_in> <schema_in> <out>");
            System.exit(2);
        }

        //conf.set("inputstring", args[0]);

        conf.set("schema", otherArgs[1]);
        Job job = new Job(conf, "TableConvert");

        job.setJarByClass(TableConvert.class); 
        job.setMapperClass(TableConvertMapper.class);
        //job.setReducerClass(TableConvertReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
