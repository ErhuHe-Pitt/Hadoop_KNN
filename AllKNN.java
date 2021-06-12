import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import com.google.gson.Gson;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.io.IOUtils;

public class AllKNN {
    private int K;
    private int N;
    private int R;
    private String inputPath;
    private String outputPath;
    QuadTree tree;

    // Xmin, Xmax, Ymin, Ymax values of the dataset
    private final float X_Min = 0;
    private final float Y_Min = 0;
    private final float X_Max = 4000;
    private final float Y_Max = 4000;

    public static void main(String args[]) {
        if (args.length != 5) {
            System.out.println("KNN expects 3 arguments:");
            System.out.println("1) HDFS Input Path");
            System.out.println("2) HDFS Output Path");
            System.out.println("3) K");
            System.out.println("4) N");
            System.out.println("5) Number of Reduce Tasks");
            System.exit(0);
        }

        AllKNN a = new AllKNN(args);

        try {
            // Delete temp HDFS folders if any exists
            Runtime.getRuntime().exec("hadoop fs -rmr meu6_erh108_temp1");
            Runtime.getRuntime().exec("hadoop fs -rmr meu6_erh108_temp2");
            Runtime.getRuntime().exec("hadoop fs -rmr meu6_erh108_temp3");
            Runtime.getRuntime().exec("hadoop fs -rmr " + args[1]);

            long start = System.currentTimeMillis();
            a.run1();
            a.merge();
            double part1 = (System.currentTimeMillis() - start) / 1000.0;
            start = System.currentTimeMillis();

            a.run2();
            double part2 = (System.currentTimeMillis() - start) / 1000.0;
            start = System.currentTimeMillis();

            a.run3();
            double part3 = (System.currentTimeMillis() - start) / 1000.0;
            start = System.currentTimeMillis();

            a.run4();
            double part4 = (System.currentTimeMillis() - start) / 1000.0;

            System.out.println(String.format("Part 1 took %.4f seconds.", part1));
            System.out.println(String.format("Part 2 took %.4f seconds.", part2));
            System.out.println(String.format("Part 3 took %.4f seconds.", part3));
            System.out.println(String.format("Part 4 took %.4f seconds.", part4));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public AllKNN(String [] args) {
        K = Integer.parseInt(args[2]);
        N = Integer.parseInt(args[3]);
        R = Integer.parseInt(args[4]);

        inputPath = args[0];
        outputPath = args[1];

        tree = new QuadTree(N, X_Min, Y_Min, X_Max, Y_Max);
    }

    /**
     * MapReduce I: Produces histogram information for cells. cell_id -> #_of_points
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public void run1() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Gson g = new Gson();

        conf.setInt("K", K);
        conf.setInt("N", N);
        conf.setInt("R", R);

        conf.set("tree", g.toJson(tree));

        Job j1 = new Job (conf,"MapReduce_1");

        j1.setJarByClass(AllKNN.class);
        j1.setMapperClass(Map1.class);
        j1.setReducerClass(Reduce1.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(IntWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);
        j1.setNumReduceTasks(R);
        FileInputFormat.addInputPath(j1, new Path(inputPath));
        FileOutputFormat.setOutputPath(j1, new Path("meu6_erh108_temp1"));

        if (!j1.waitForCompletion(true)) {
            System.exit(0);
        }

        // Read output from HDFS
        FileSystem fs = FileSystem.get(j1.getConfiguration());
        FileStatus[] list = fs.listStatus(FileOutputFormat.getOutputPath(j1));
        FSDataInputStream in = null;

        for (FileStatus status : list) {
            in = fs.open(status.getPath());
            String out = IOUtils.toString(in, "UTF-8");
            String[] lines = out.split("\n");

            // Add histogram information to cells. Each cell will know how many points in it.
            for(String line : lines) {
                String [] tok = line.split("\\s+");
                if (tok.length == 2)
                    tree.addPoints(tok[0], Integer.parseInt(tok[1]));
            }
        }

        Runtime.getRuntime().exec("hadoop fs -rmr meu6_erh108_temp1");
    }

    /**
     * MapReduce II: First part of KNN calculation. Outputs (id, coord, cell_id) -> KNN_List.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void run2() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Gson g = new Gson();

        conf.setInt("K", K);
        conf.setInt("N", N);
        conf.setInt("R", R);

        conf.set("tree", g.toJson(tree));

        Job j2 = new Job (conf,"MapReduce_2");

        j2.setJarByClass(AllKNN.class);
        j2.setMapperClass(Map2.class);
        j2.setReducerClass(Reduce2.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(Text.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        j2.setNumReduceTasks(R);
        FileInputFormat.addInputPath(j2, new Path(inputPath));
        FileOutputFormat.setOutputPath(j2, new Path("meu6_erh108_temp2"));

        if (!j2.waitForCompletion(true)) {
            System.exit(0);
        }
    }

    /**
     * MapReduce III: Second part of All KNN calculation. Outputs (id, coord, cell_id) -> KNN_List.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void run3() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Gson g = new Gson();

        conf.setInt("K", K);
        conf.setInt("N", N);
        conf.setInt("R", R);

        conf.set("tree", g.toJson(tree));

        Job j3 = new Job (conf,"MapReduce_3");

        j3.setJarByClass(AllKNN.class);
        j3.setMapperClass(Map3.class);
        j3.setReducerClass(Reduce3.class);
        j3.setMapOutputKeyClass(Text.class);
        j3.setMapOutputValueClass(Text.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(Text.class);
        j3.setNumReduceTasks(R);
        FileInputFormat.addInputPath(j3, new Path("meu6_erh108_temp2"));
        FileOutputFormat.setOutputPath(j3, new Path("meu6_erh108_temp3"));

        if (!j3.waitForCompletion(true)) {
            System.exit(0);
        }

        Runtime.getRuntime().exec("hadoop fs -rmr meu6_erh108_temp2");
    }

    /**
     * MapReduce 4: Last part of the All KNN calculation. Merges different lists for a point. Outputs point_id -> KNN_List
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void run4() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Gson g = new Gson();

        conf.setInt("K", K);
        conf.setInt("N", N);
        conf.setInt("R", R);

        Job j4 = new Job (conf,"MapReduce_4");

        j4.setJarByClass(AllKNN.class);
        j4.setMapperClass(Map4.class);
        j4.setReducerClass(Reduce4.class);
        j4.setMapOutputKeyClass(Text.class);
        j4.setMapOutputValueClass(Text.class);
        j4.setOutputKeyClass(Text.class);
        j4.setOutputValueClass(Text.class);
        j4.setNumReduceTasks(R);
        FileInputFormat.addInputPath(j4, new Path("meu6_erh108_temp3"));
        FileOutputFormat.setOutputPath(j4, new Path(outputPath));

        if (!j4.waitForCompletion(true)) {
            System.exit(0);
        }

        Runtime.getRuntime().exec("hadoop fs -rmr meu6_erh108_temp3");
    }

    public void merge() {
        tree.merge(K + 1);
    }

    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable>{
        private QuadTree t;
        private Text textKey;
        private IntWritable one = new IntWritable(1);

        protected void setup(Context context) {
            // Deserialize quadtree
            Configuration conf = context.getConfiguration();
            Gson g = new Gson();
            t = g.fromJson(conf.get("tree"), QuadTree.class);
        }

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            // Calculate cell id for a point and print cell_id -> 1
            String[] PointInfo = value.toString().split(",");
            String Point_id = PointInfo[0];
            float Point_x = Float.parseFloat(PointInfo[1]);
            float Point_y = Float.parseFloat(PointInfo[2]);
            Point p = new Point(Point_id, Point_x, Point_y);
            textKey = new Text(t.findId(t.root, p));
            con.write(textKey, one);
        }
    }


    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text text = new Text();

        public void reduce(Text key, Iterable<IntWritable> counts, Context con) throws IOException, InterruptedException {
            // Sum all cell_id -> 1 entries by cell_id
            int sum = 0;

            for (IntWritable count : counts) {
                sum += count.get();
            }

            con.write(key, new IntWritable(sum));
        }
    }


    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        private QuadTree t;
        private Text textKey;

        protected void setup(Context context) {
            // Deserialize quadtree
            Configuration conf = context.getConfiguration();
            Gson g = new Gson();
            t = g.fromJson(conf.get("tree"), QuadTree.class);
        }

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // Calculate cell id for the point and write cell_id -> (point_id, coord)
            String[] PointInfo = value.toString().split(",");
            String Point_id = PointInfo[0];
            float Point_x = Float.parseFloat(PointInfo[1]);
            float Point_y = Float.parseFloat(PointInfo[2]);
            Point p = new Point(Point_id, Point_x, Point_y);
            textKey = new Text(t.findId(t.root, p));
            con.write(textKey, value);
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
        private int K;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            K = conf.getInt("K", 0);
        }

        public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            // For all cell_id, reduce points into an arraylist. For all points
            // in the arraylist, calculate K nearest neighbors in the same cell.

            ArrayList<Point> pl = new ArrayList<Point>();

            for (Text t : values) {
                String[] line = t.toString().split(",");
                Point data = new Point(line[0], Float.parseFloat(line[1]), Float.parseFloat(line[2]));
                pl.add(data);
            }

            for (Point element : pl) {
                String res;
                Neighbor nbor = new Neighbor(element, pl, K);
                res = nbor.DistanceList();
                // output
                String kt=element.getId()+","+element.getX()+","+element.getY()+ "," + key.toString();
                Text k = new Text();
                k.set(kt);
                String vt = res;
                Text v = new Text();
                v.set(vt);
                //
                con.write(k, v);
            }
        }
    }

    public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
        private QuadTree t;
        private Text textKey;

        protected void setup(Context context) {
            // Deserialize the quadtree
            Configuration conf = context.getConfiguration();
            Gson g = new Gson();
            t = g.fromJson(conf.get("tree"), QuadTree.class);
        }

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String [] valArr = value.toString().split("\\s+")[1].split(",");
            String [] keyArr = value.toString().split("\\s+")[0].split(",");
            float maxDist = Float.parseFloat(valArr[valArr.length - 1]);
            Cell c = t.getCell(keyArr[3]);
            float px = Float.parseFloat(keyArr[1]);
            float py = Float.parseFloat(keyArr[2]);

            // Write cell_id -> (point_id, coord, ERHANERHU). ERHANERHU is a dummy word
            // to distinguish between two different map outputs.
            con.write(new Text(keyArr[3]), new Text(String.format("%s,%s,%s,%s", keyArr[0], keyArr[1], keyArr[2], "ERHANERHU")));

            // Check if we need to draw the circle.
            if (px + maxDist > c.getX2() || px - maxDist < c.getX1() || py + maxDist > c.getY2() || py - maxDist < c.getY1()) {
                Text k;
                // Get all cell ids intersect with the circle x, y, maxDistance
                ArrayList<String> intersect = t.getIntersections(px, py, maxDist);

                // Do not flag the cell_id where the point originally resides with false.
                // We dont have to do KNN calculation for that cell, because we did in Part 2.
                for (String inter : intersect) {
                    k = new Text(inter);
                    con.write(k, new Text(String.format("%s,%s,%s,", keyArr[0], keyArr[1], keyArr[2]) + String.join(",", valArr) + "," + inter.equals(keyArr[3])));
                }
            } else {
                // No need to draw the circle
                con.write(new Text(keyArr[3]), new Text(String.format("%s,%s,%s,", keyArr[0], keyArr[1], keyArr[2]) + String.join(",", valArr) + ",true"));
            }
        }
    }

    public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
        private int K;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            K = conf.getInt("K", 0);
        }

        public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            ArrayList<Point> points = new ArrayList<Point>();
            ArrayList<String> falses = new ArrayList<String>();

            // Fill points list for this cell id with ERHANERHU entries
            // Also keep track of "false" flagged entries
            for (Text value : values) {
                String [] tokens = value.toString().split(",");
                if (tokens.length == 4 && tokens[3].equals("ERHANERHU")) {
                    points.add(new Point(tokens[0], Float.parseFloat(tokens[1]), Float.parseFloat(tokens[2])));
                } else {
                    if (Boolean.parseBoolean(tokens[tokens.length - 1])) {
                        con.write(new Text(String.format("%s,%s,%s", tokens[0], tokens[1], tokens[2])), new Text(key.toString() + "," + String.join(",", Arrays.copyOfRange(tokens, 3, tokens.length - 1))));
                    } else {
                        falses.add(key.toString() + "#" + value.toString());
                    }
                }
            }

            // For all "false" flagged entry, calculate KNN list in this cell id and merge list with the original one.
            for (String s : falses) {
                String [] tok1 = s.split("#");
                String cellID = tok1[0];
                String [] tok2 = tok1[1].split(",");

                Point p = new Point(tok2[0], Float.parseFloat(tok2[1]), Float.parseFloat(tok2[2]));

                Neighbor nbor = new Neighbor(p, points, K);
                String list = nbor.DistanceList();
                String orgList = String.join(",", Arrays.copyOfRange(tok2, 3, tok2.length - 1));
                String merged = MergeList.merge(list, orgList, K);

                con.write(new Text(String.format("%s,%s,%s", tok2[0], tok2[1], tok2[2])), new Text(key.toString() + "," + merged));
            }
        }
    }

    public static class Map4 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // Write point_id -> KNN_List entries from previous MapReduce output
            String[] keyArr = value.toString().split("\\s+")[0].split(",");
            String[] valArr = value.toString().split("\\s+")[1].split(",");
            con.write(new Text(keyArr[0]), new Text(String.join(",", Arrays.copyOfRange(valArr, 1, valArr.length))));
        }
    }

    public static class Reduce4 extends Reducer<Text, Text, Text, Text> {
        private int K;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            K = conf.getInt("K", 0);
        }

        public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            // Get mapper output, and reduce different KNN Lists with the same key (point_id)
            // into one list.
            ArrayList<String> distances = new ArrayList<String>();
            for (Text val : values) {
                distances.add(val.toString());
            }

            if (distances.size() == 1) {
                con.write(key, new Text(distances.get(0)));
            } else if (distances.size() > 1) {
                String merged = distances.get(0);

                for (int i = 1; i < distances.size(); i++)
                    merged = MergeList.merge(merged, distances.get(i), K);

                con.write(key, new Text(merged));
            } else {
                System.out.println("Empty list?");
            }
        }
    }
}