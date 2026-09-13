package bigdata.bai14;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Bài 14 – Kiểm tra số nguyên tố: đếm số lượng các số nguyên tố có trong file dữ liệu.
 *
 *   map     : với mỗi số n trong file
 *               -> ("TONG_SO_LUONG_SO_NGUYEN", 1)
 *               -> ("SO_LUONG_SO_NGUYEN_TO", 1)   nếu n là số nguyên tố
 *   combine : cộng dồn cục bộ ở phía map (giảm dữ liệu shuffle xuống còn 2 cặp / map task)
 *   reduce  : 1 reducer duy nhất cộng tổng cho từng key
 *
 * Mỗi lần xuất hiện được đếm riêng (file có "7 7" thì tính là 2 số nguyên tố).
 * Kiểm tra nguyên tố bằng Miller–Rabin tất định với 12 cơ sở nguyên tố đầu tiên,
 * cho kết quả chính xác với mọi giá trị kiểu long.
 */
public class PrimeCount extends Configured implements Tool {

  public enum Stats { VALID_TOKENS, INVALID_TOKENS }

  static final String PRIME_KEY = "SO_LUONG_SO_NGUYEN_TO";
  static final String TOTAL_KEY = "TONG_SO_LUONG_SO_NGUYEN";

  private static final long[] BASES = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37};

  /** a*b không tràn long khi cả hai toán hạng nhỏ hơn giá trị này (≈ sqrt(2^63)). */
  private static final long SAFE_MUL_LIMIT = 3_037_000_499L;

  static boolean isPrime(long n) {
    if (n < 2) {
      return false;
    }
    for (long p : BASES) {
      if (n == p) {
        return true;
      }
      if (n % p == 0) {
        return false;
      }
    }
    // n lẻ, n > 37: viết n - 1 = d * 2^s với d lẻ
    long d = n - 1;
    int s = Long.numberOfTrailingZeros(d);
    d >>= s;
    for (long a : BASES) {
      if (!millerRabinRound(n, a, d, s)) {
        return false;
      }
    }
    return true;
  }

  private static boolean millerRabinRound(long n, long a, long d, int s) {
    long x = powMod(a, d, n);
    if (x == 1 || x == n - 1) {
      return true;
    }
    for (int r = 1; r < s; r++) {
      x = mulMod(x, x, n);
      if (x == n - 1) {
        return true;
      }
    }
    return false;
  }

  private static long powMod(long base, long exp, long m) {
    long result = 1;
    base %= m;
    while (exp > 0) {
      if ((exp & 1) == 1) {
        result = mulMod(result, base, m);
      }
      base = mulMod(base, base, m);
      exp >>= 1;
    }
    return result;
  }

  private static long mulMod(long a, long b, long m) {
    if (m <= SAFE_MUL_LIMIT) {
      return a * b % m;
    }
    return BigInteger.valueOf(a).multiply(BigInteger.valueOf(b))
        .mod(BigInteger.valueOf(m)).longValue();
  }

  // ------------------------------ MapReduce ----------------------------

  public static class PrimeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1);
    private final Text primeKey = new Text(PRIME_KEY);
    private final Text totalKey = new Text(TOTAL_KEY);

    @Override
    protected void map(LongWritable offset, Text line, Context ctx)
        throws IOException, InterruptedException {
      for (String token : line.toString().split("[\\s,;]+")) {
        if (token.isEmpty()) {
          continue;
        }
        long n;
        try {
          n = Long.parseLong(token);
        } catch (NumberFormatException e) {
          ctx.getCounter(Stats.INVALID_TOKENS).increment(1);
          continue;
        }
        ctx.getCounter(Stats.VALID_TOKENS).increment(1);
        ctx.write(totalKey, ONE);
        if (isPrime(n)) {
          ctx.write(primeKey, ONE);
        }
      }
    }

    /** Luôn phát (key, 0) để kết quả vẫn có dòng "SO_LUONG_SO_NGUYEN_TO 0" khi không có số nguyên tố. */
    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
      LongWritable zero = new LongWritable(0);
      ctx.write(primeKey, zero);
      ctx.write(totalKey, zero);
    }
  }

  /** Dùng làm cả combiner và reducer. */
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
      System.err.println("Cach dung: PrimeCount <input> <output>");
      return 2;
    }
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);

    Job job = Job.getInstance(getConf(), "Bai 14 - Dem so luong so nguyen to");
    job.setJarByClass(PrimeCount.class);
    job.setMapperClass(PrimeMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    if (!job.waitForCompletion(true)) {
      return 1;
    }

    Map<String, Long> result = readResult(output);
    long invalid = job.getCounters().findCounter(Stats.INVALID_TOKENS).getValue();
    System.out.println();
    System.out.println("==================== KET QUA BAI 14 ====================");
    System.out.printf("Input                             : %s%n", input);
    System.out.printf("Tong so luong so nguyen trong file: %,d%n", result.getOrDefault(TOTAL_KEY, 0L));
    System.out.printf("So token khong hop le (bo qua)    : %,d%n", invalid);
    System.out.printf("SO LUONG SO NGUYEN TO             : %,d%n", result.getOrDefault(PRIME_KEY, 0L));
    System.out.printf("File ket qua                      : %s%n", new Path(output, "part-r-00000"));
    System.out.println("========================================================");
    return 0;
  }

  private Map<String, Long> readResult(Path dir) throws IOException {
    Map<String, Long> result = new LinkedHashMap<>();
    FileSystem fs = dir.getFileSystem(getConf());
    for (FileStatus part : fs.globStatus(new Path(dir, "part-r-*"))) {
      try (BufferedReader in = new BufferedReader(
          new InputStreamReader(fs.open(part.getPath()), StandardCharsets.UTF_8))) {
        String line;
        while ((line = in.readLine()) != null) {
          String[] kv = line.split("\t");
          if (kv.length == 2) {
            result.put(kv[0], Long.parseLong(kv[1]));
          }
        }
      }
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new PrimeCount(), args));
  }
}
