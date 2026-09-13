package bigdata.bai07;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Bài 7 – Đếm số duy nhất: đọc file gồm các số nguyên, in ra số lượng các số
 * nguyên chỉ xuất hiện đúng 1 lần.
 *
 * Chuỗi 2 job MapReduce:
 *
 *   Job 1 (lọc số duy nhất)
 *     map     : mỗi số n trong file          -> (n, 1)
 *     combine : cộng dồn cục bộ               -> (n, số lần xuất hiện trong split)
 *     reduce  : tổng số lần xuất hiện của n; chỉ ghi n ra nếu tổng == 1
 *
 *   Job 2 (đếm)
 *     map     : mỗi dòng output của Job 1     -> ("SO_DUY_NHAT", 1)
 *     combine : cộng dồn cục bộ
 *     reduce  : 1 reducer duy nhất            -> ("SO_DUY_NHAT", tổng)
 *
 * Dữ liệu vào: các số nguyên (kiểu long, có thể âm) phân tách bởi khoảng trắng,
 * dấu phẩy hoặc dấu chấm phẩy. "007" và "7" được coi là cùng một số. Token không
 * phải số nguyên hợp lệ được bỏ qua và đếm vào counter INVALID_TOKENS.
 */
public class UniqueNumberCount extends Configured implements Tool {

  public enum Stats { VALID_TOKENS, INVALID_TOKENS, DISTINCT_NUMBERS, UNIQUE_NUMBERS }

  static final String RESULT_KEY = "SO_DUY_NHAT";
  private static final LongWritable ONE = new LongWritable(1);

  // ------------------------------- Job 1 -------------------------------

  public static class NumberMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    private final LongWritable number = new LongWritable();

    @Override
    protected void map(LongWritable offset, Text line, Context ctx)
        throws IOException, InterruptedException {
      for (String token : line.toString().split("[\\s,;]+")) {
        if (token.isEmpty()) {
          continue;
        }
        try {
          number.set(Long.parseLong(token));
        } catch (NumberFormatException e) {
          ctx.getCounter(Stats.INVALID_TOKENS).increment(1);
          continue;
        }
        ctx.getCounter(Stats.VALID_TOKENS).increment(1);
        ctx.write(number, ONE);
      }
    }
  }

  /** Dùng làm combiner cho Job 1: cộng dồn số lần xuất hiện ngay ở phía map. */
  public static class FrequencyCombiner
      extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    private final LongWritable sum = new LongWritable();

    @Override
    protected void reduce(LongWritable number, Iterable<LongWritable> counts, Context ctx)
        throws IOException, InterruptedException {
      long total = 0;
      for (LongWritable c : counts) {
        total += c.get();
      }
      sum.set(total);
      ctx.write(number, sum);
    }
  }

  public static class UniqueFilterReducer
      extends Reducer<LongWritable, LongWritable, LongWritable, NullWritable> {
    @Override
    protected void reduce(LongWritable number, Iterable<LongWritable> counts, Context ctx)
        throws IOException, InterruptedException {
      long total = 0;
      for (LongWritable c : counts) {
        total += c.get();
      }
      ctx.getCounter(Stats.DISTINCT_NUMBERS).increment(1);
      if (total == 1) {
        ctx.getCounter(Stats.UNIQUE_NUMBERS).increment(1);
        ctx.write(number, NullWritable.get());
      }
    }
  }

  // ------------------------------- Job 2 -------------------------------

  public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final Text key = new Text(RESULT_KEY);

    @Override
    protected void map(LongWritable offset, Text line, Context ctx)
        throws IOException, InterruptedException {
      if (!line.toString().trim().isEmpty()) {
        ctx.write(key, ONE);
      }
    }

    /** Luôn phát (key, 0) để kết quả vẫn có dòng "SO_DUY_NHAT 0" khi không có số duy nhất nào. */
    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
      ctx.write(key, new LongWritable(0));
    }
  }

  /** Dùng làm cả combiner và reducer cho Job 2. */
  public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final LongWritable sum = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context ctx)
        throws IOException, InterruptedException {
      long total = 0;
      for (LongWritable v : values) {
        total += v.get();
      }
      sum.set(total);
      ctx.write(key, sum);
    }
  }

  // ------------------------------- Driver ------------------------------

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Cach dung: UniqueNumberCount <input> <output>");
      return 2;
    }
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    Path uniqueDir = new Path(output, "1-unique-numbers");
    Path countDir = new Path(output, "2-unique-count");

    Job filter = Job.getInstance(getConf(), "Bai 7 - Job 1: loc cac so xuat hien dung 1 lan");
    filter.setJarByClass(UniqueNumberCount.class);
    filter.setMapperClass(NumberMapper.class);
    filter.setCombinerClass(FrequencyCombiner.class);
    filter.setReducerClass(UniqueFilterReducer.class);
    filter.setMapOutputKeyClass(LongWritable.class);
    filter.setMapOutputValueClass(LongWritable.class);
    filter.setOutputKeyClass(LongWritable.class);
    filter.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(filter, input);
    FileOutputFormat.setOutputPath(filter, uniqueDir);
    if (!filter.waitForCompletion(true)) {
      return 1;
    }

    Job count = Job.getInstance(getConf(), "Bai 7 - Job 2: dem so luong so duy nhat");
    count.setJarByClass(UniqueNumberCount.class);
    count.setMapperClass(CountMapper.class);
    count.setCombinerClass(SumReducer.class);
    count.setReducerClass(SumReducer.class);
    count.setNumReduceTasks(1);
    count.setOutputKeyClass(Text.class);
    count.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(count, uniqueDir);
    FileOutputFormat.setOutputPath(count, countDir);
    if (!count.waitForCompletion(true)) {
      return 1;
    }

    long uniqueCount = readResult(countDir);
    org.apache.hadoop.mapreduce.Counters c = filter.getCounters();
    System.out.println();
    System.out.println("==================== KET QUA BAI 7 ====================");
    System.out.printf("Input                                  : %s%n", input);
    System.out.printf("So token la so nguyen hop le           : %,d%n", c.findCounter(Stats.VALID_TOKENS).getValue());
    System.out.printf("So token khong hop le (bo qua)         : %,d%n", c.findCounter(Stats.INVALID_TOKENS).getValue());
    System.out.printf("So gia tri phan biet                   : %,d%n", c.findCounter(Stats.DISTINCT_NUMBERS).getValue());
    System.out.printf("SO LUONG SO CHI XUAT HIEN DUNG 1 LAN   : %,d%n", uniqueCount);
    System.out.printf("Danh sach cac so do                    : %s%n", uniqueDir);
    System.out.println("=======================================================");
    return 0;
  }

  /** Đọc dòng "SO_DUY_NHAT<TAB>n" do reducer duy nhất của Job 2 ghi ra. */
  private long readResult(Path dir) throws IOException {
    FileSystem fs = dir.getFileSystem(getConf());
    for (FileStatus part : fs.globStatus(new Path(dir, "part-r-*"))) {
      try (BufferedReader in = new BufferedReader(
          new InputStreamReader(fs.open(part.getPath()), StandardCharsets.UTF_8))) {
        String line;
        while ((line = in.readLine()) != null) {
          String[] kv = line.split("\t");
          if (kv.length == 2 && kv[0].equals(RESULT_KEY)) {
            return Long.parseLong(kv[1]);
          }
        }
      }
    }
    throw new IOException("Khong tim thay ket qua trong " + dir);
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new UniqueNumberCount(), args));
  }
}
