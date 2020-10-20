
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.StringTokenizer;

import org.apache.hadoop.commons.io.FilesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weka.core.Instances;
import weka.core.ArffReader;

class KNN {

    public static class MapperKNN extends Mapper<> {

        private static void computeKNN() {

        }

        @Override
        public void setup(Context context) throws IOException {
            
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
                String params = FilesUtils.readFileToString(new File("./paramFile"));
                StringTokenizer tokenizer = new StringTokenizer(params, ",");

                k = Integer.parseInt(tokenizer.nextToken());
            }
        }
        

        @Override
        public static void map(Context context) throws IOException {
        /*
         *
         * for i in range(testSet)
         *      ComputeKNN(dataPoint, i, TrainSetSplitj, k)
         *      for n in range(0, k)
         *          CDj[i][n] = (class of neighbor n, distance of neighbor n)
         * 
         * key = idMapper
         * context.write(key, CDj)
         */

            /*
            for (int i = 0; i < testSet.numInstancesj(); i++) {
                computeKNN(testSet[i], trainSetSplit_j, k);

                for (int n = 0; n < k; n++) {
                    CD_j[i][n] = new Tuple<Integer, Double>();
                }
            }

            key = idMapper;
            context.write(key, CD_j);
            */
        }
    }

    public static class ReducerKNN extends Reducer<> {

        private static double CD_reducer[][];

        private static int majorityVoting() {
            return 0;
        }

        @Override
        public static void setup(Context context) throws IOException {
        /*
         * 
         * create CD reducer matrix of size (size(testSet * k))
         * 
         * Classes can be random but the distances should be set to infinity
         */

           CD_reducer = new double[testSet.size()][k]; 

        }

        @Override
        public static void reduce(Context context) throws IOException {
        /*
         *
         * for i in range(testSet.size())
         *      cont = 0
         *      for n in range(0, k)
         *          if CDj[i][cont].dist < CD_reducer[i][n].dist
         *              CD_reducer[i][n] = CDj[i][cont]
         *              cont++
         * 
         */

            /*
            for (int i = 0; i < testSet.numInstances(); i++) {
                int cont = 0;
                for (int n = 0; n < k; n++) {
                    if (CD_j[i][cont].dist < CD_reducer[i][n].dist) {
                        CD_reducer[i][n] = CD_j[i][cont];
                        cont++;
                    }
                }
            }
            */
        }

        @Override
        public static void cleanup(Context context) throws IOException {
        /*
         *
         * for i in range(0, size(testSet))
         *      PredClass[i] = majorityVoting(classes(CD_reducer))
         *      key = i
         *      context.write(key, PredClass[i])
         *
         */

            /*
            for (int i = 0; i < testSet.numInstances(); i++) {
                prediction = majorityVoting();
                context.write(i, prediction);
            }
            */

        }

    }

    public static void main(String[] argv) {

        BufferedReader reader = new BufferedReader(new FileReader("./small.arff"));
        ArffReader arff = new ArffReader(reader);

        Instances data = arff.getData();
        data.setClassIndex(data.numAttributes() - 1);

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "KNN");
        job.setJarByClass(KNN.class);

        job.setMapperClass(MapperKNN.class);
        job.setReducerClass(ReducerKNN.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWriteable.class);
        job.setMapOutputValueClass(DoubleString.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.addOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
