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
public class TableMerge{

    public static class TableMergeMapper extends Mapper<LongWritable, Text, Text, Text> {

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
            
            int j = 0;
            for ( int i = 0 ; i < vals.length ; i++ ) {
                String[] pairs = vals[i].split("=");
                if ( j >= mPKIdx.size() ) 
                    break;
                int idx = mPKIdx.get(j);
                String pKey = mPrimaryKeys.get(idx);
                if ( pairs[0].equals( pKey ) ) {
                    if ( j != 0 ) {
                        sbStr.append("-");
                    }
                    sbStr.append(pairs[1]);
                    j++;
                }
            }
            interValue.set(rawString);
            interKey.set(sbStr.toString());
            context.write( interKey , interValue);

             
        }// end of map

    }

    public static class TableMergeReducer extends Reducer<Text, Text, NullWritable, Text> {

        private ArrayList<String> mColumns = new ArrayList<String>();
        private ArrayList<String> mValues = new ArrayList<String>();
        private ArrayList<String> mPrimaryKeys = new ArrayList<String>();
        private ArrayList<Integer> mPKIdx = new ArrayList<Integer>();
        private Text interValue = new Text();
        private StringBuffer mStrBuf = new StringBuffer();
        //----------------------------------------------------------------------------
        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {

            FileSystem dfs = FileSystem.get(context.getConfiguration());
            Path path = new Path(context.getConfiguration().get("schema"));

            FSDataInputStream is = dfs.open(path);


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
        private int findIdx(String col) {
            for ( int i = 0 ; i < mColumns.size() ; i++ ) {
                if ( col.equals(mColumns.get(i)) ) {
                    return i;
                }
            }
            return -1;
        }
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
            try {
                mValues.clear();
                for ( int i = 0 ; i < mColumns.size() ; i++ ) {
                    mValues.add("");                            
                }

                for( Text val : values){
                    String rawString = val.toString();
                    String vals[] = rawString.split(",");
                    for ( int i = 0; i < vals.length ; i++ ) {
                        String[] pairs = vals[i].split("=");
                        int idx = findIdx(pairs[0]);
                        if ( idx >= 0 ) {
                            mValues.set(idx,pairs[1]);
                        }
                    }
                }
                mStrBuf.setLength(0);
                for ( int i = 0 ; i < mValues.size() ; i++ ) {
                    if ( i != 0 ) mStrBuf.append(",");
                    mStrBuf.append(mValues.get(i));
                }
                interValue.set(mStrBuf.toString());
                context.write(NullWritable.get(), interValue);

            } catch ( Exception e ) {
            }
            
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: "+ TableMerge.class.getName() +" <inputs>+ <out_schema> <out_folder>");
            System.exit(2);
        }

        //conf.set("inputstring", args[0]);
        StringBuilder sb = new StringBuilder();
    
        for ( int i = 0 ; i < otherArgs.length-2 ; i++ ) {
            if ( i != 0 ) 
                sb.append(",");
            sb.append(otherArgs[i]);
        }
        conf.set("schema", otherArgs[otherArgs.length-2]);
        Job job = new Job(conf, "TableConvert");

        job.setJarByClass(TableMerge.class); 
        job.setMapperClass(TableMergeMapper.class);
        job.setReducerClass(TableMergeReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(4);

        //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPaths(job, sb.toString());
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
