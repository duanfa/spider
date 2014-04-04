package hadoop;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class DoubanFindFileInputFormat extends InputFormat<Text, Text> {
	private static final String INPUT_DIR = "mapreduce.input.doubanFindFileInputFormat.inputdir";

	static class DoubanInputSplit extends InputSplit implements Writable {
		String path;

		public DoubanInputSplit() {
		}

		public DoubanInputSplit(String path) {
			this.path = path;
		}

		public long getLength() throws IOException {
			return 0;
		}

		public String[] getLocations() throws IOException {
			return new String[] {};
		}

		public void readFields(DataInput in) throws IOException {
			path = WritableUtils.readString(in);
		}

		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, path);
		}
	}

	/**
	 * A record reader that will generate a range of numbers.
	 */
	static class DoubanRecordReader extends RecordReader<Text, Text> {
		Text filePath;
		TaskAttemptContext context;

		public DoubanRecordReader() {
		}

		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			filePath = new Text(((DoubanInputSplit) split).path);
			this.context = context;
		}

		public void close() throws IOException {
			// NOTHING
		}

		public Text getCurrentKey() {

			return filePath;
		}

		public Text getCurrentValue() {
			Text text = new Text("");
			try {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				FSDataInputStream hdfsInStream = fs.open(new Path(filePath.toString()));
				BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
				String line = "";
				StringBuffer sb = new StringBuffer();
				while ((line = br.readLine()) != null) {
					sb.append(line + "\n");
				}
				text.set(sb.toString());
			} catch (IOException e) {
				e.printStackTrace();
			}
			return text;
		}

		public float getProgress() throws IOException {
			// return finishedRows / (float) totalRows;
			return 1;
		}

		public boolean nextKeyValue() {
			return false;
		}

	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new DoubanRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) {
		Configuration conf = job.getConfiguration();
		String dir = conf.get(INPUT_DIR);
		Path path = new Path(dir);
		Set<String> paths = getAllFilePaths(job, path);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (String p : paths) {
			splits.add(new DoubanInputSplit(p));
		}
		System.out.println("splits size:"+splits.size());
		return splits;
	}

	private Set<String> getAllFilePaths(JobContext job, Path path) {
		Set<String> paths = new HashSet<String>();
		try {
			FileSystem fs = FileSystem.get(job.getConfiguration());
			FileStatus stats[] = fs.listStatus(path);
			for (FileStatus statu : stats) {
				if (statu.isFile()) {
					paths.add(statu.getPath().toString());
				} else {
					paths.addAll(getAllFilePaths(job, statu.getPath()));
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return paths;
	}

	public static void addInputPath(Job job, String path) {
		Configuration conf = job.getConfiguration();
		conf.set(INPUT_DIR, path);
	}

}