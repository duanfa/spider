package fileSplit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class Spliter {
	public static void main(String[] args) {
		File wf = new File(
				"C:/Users/admin/Downloads/data-status-cal_status_1-calstatus.log");
		final CountWords cw1 = new CountWords(wf, 0, wf.length() / 2);
		final CountWords cw2 = new CountWords(wf, wf.length() / 2, wf.length());
		final Thread t1 = new Thread(cw1);
		final Thread t2 = new Thread(cw2);
		// ���������̷ֱ߳����ļ��Ĳ�ͬƬ��
		t1.start();
		t2.start();
		Thread t = new Thread() {
			public void run() {
				while (true) {
					// �����߳̾����н���
					if (Thread.State.TERMINATED == t1.getState()
							&& Thread.State.TERMINATED == t2.getState()) {
						// ��ȡ���Դ���Ľ��
						HashMap<String, Integer> hMap1 = cw1.getResult();
						HashMap<String, Integer> hMap2 = cw2.getResult();
						// ʹ��TreeMap��֤�������
						TreeMap<String, Integer> tMap = new TreeMap<String, Integer>();
						// �Բ�ͬ�̴߳���Ľ����������
						tMap.putAll(hMap1);
						tMap.putAll(hMap2);
						// ��ӡ������鿴���
						for (Map.Entry<String, Integer> entry : tMap.entrySet()) {
							String key = entry.getKey();
							int value = entry.getValue();
							System.out.println(key + ":\t" + value);
						}
						// ��������浽�ļ���
						mapToFile(tMap, new File("result.txt"));
					}
					return;
				}
			}
		};
		t.start();
	}

	// ��������� "���ʣ�����" ��ʽ�����ļ���
	private static void mapToFile(Map<String, Integer> src, File dst) {
		try {
			// �Խ�Ҫд����ļ�����ͨ��
			FileChannel fcout = new FileOutputStream(dst).getChannel();
			// ʹ��entrySet�Խ�������б���
			for (Map.Entry<String, Integer> entry : src.entrySet()) {
				String key = entry.getKey();
				int value = entry.getValue();
				// ���������ָ����ʽ�ŵ���������
				ByteBuffer bBuf = ByteBuffer.wrap((key + ":\t" + value)
						.getBytes());
				fcout.write(bBuf);
				bBuf.clear();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class CountWords implements Runnable {

	private FileChannel fc;
	private FileLock fl;
	private MappedByteBuffer mbBuf;
	private HashMap<String, Integer> hm;

	public CountWords(File src, long start, long end) {
		try {
			// �õ���ǰ�ļ���ͨ��
			fc = new RandomAccessFile(src, "rw").getChannel();
			// ������ǰ�ļ��Ĳ���
			fl = fc.lock(start, end, false);
			// �Ե�ǰ�ļ�Ƭ�ν����ڴ�ӳ�䣬����ļ�������Ҫ�и�ɶ��Ƭ��
			mbBuf = fc.map(FileChannel.MapMode.READ_ONLY, start, end);
			// ����HashMapʵ����Ŵ�����
			hm = new HashMap<String, Integer>();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		String str = Charset.forName("UTF-8").decode(mbBuf).toString();
		// ʹ��StringTokenizer��������
		StringTokenizer token = new StringTokenizer(str);
		String word;
		while (token.hasMoreTokens()) {
			// ���������ŵ�һ��HashMap�У����ǵ��洢�ٶ�
			word = token.nextToken();
			if (null != hm.get(word)) {
				hm.put(word, hm.get(word) + 1);
			} else {
				hm.put(word, 1);
			}
		}
		try {
			// �ͷ��ļ���
			fl.release();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return;
	}

	// ��ȡ��ǰ�̵߳�ִ�н��
	public HashMap<String, Integer> getResult() {
		return hm;
	}
}