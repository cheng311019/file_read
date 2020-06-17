import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.*;
import java.util.concurrent.*;

public class Main {
    private static Boolean RUN = true;
    private static final String FILE_DIR = "src/main/resourceskk";
    //读取文件的线程组
    private static ExecutorService executorService =Executors.newFixedThreadPool(10);
    //消费队列线程，将数据添加到map中
    private static ExecutorService consumerService =Executors.newSingleThreadExecutor();
    //读取map中的数据
    private static ExecutorService readService =Executors.newSingleThreadExecutor();
    //阻塞队列，读取文件线程将每一行记录写入队列中
    static LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
    private static ConcurrentSkipListMap<Integer, Entry> map = new ConcurrentSkipListMap<>();
    private static final String PATTERN = "%d, %d, %f";
    public static void main(String[] args) {
        //读取系统文件参数
        String fileDir = System.getProperty("file.dir");
        File file = new File(fileDir == null ? FILE_DIR : fileDir);
        //获取所有文件
        File[] tempList = file.listFiles();
        if(tempList == null ){
            throw new RuntimeException("file not found");
        }
        for (File item :tempList) {
            if (item.isFile()) {
                try {
                    //将文件交由线程池处理
                    executorService.execute(new FileReader(new FileInputStream(item)));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        //消费线程，用于处理queue中的数据
        consumerService.execute(() -> {
            while (RUN) {
                String row;
                try {
                    row = Main.queue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                String[] spit = row.split(",");
                Integer groupId = Integer.valueOf(spit[1]);
                Integer id = Integer.valueOf(spit[0]);
                float quota = Float.parseFloat(spit[2]);
                if (Main.map.containsKey(groupId)) {
                    Entry entry = Main.map.get(groupId);
                    if (entry.getScore() > quota) {
                        entry.setId(id);
                        entry.setScore(quota);
                    }
                } else {
                    Main.map.put(groupId, new Entry(id, quota));
                }
            }
        });
        //读取线程,每秒读取输出一次
        readService.execute(() -> {
            while (RUN){
                System.out.println("start");
                map.forEach((groupId, entry) -> System.out.println(String.format(PATTERN, groupId, entry.getId(), entry.getScore())));
                System.out.println("end");
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });
        //读取到kill -9信号，停止服务
        Signal.handle(new Signal("KILL"), signal -> {
            RUN = false;
            executorService.shutdownNow();
            consumerService.shutdownNow();
            readService.shutdownNow();

        });
    }
}
/**
 * 读取文件里的数据，将数据添加到queue中
 * */
class FileReader implements Runnable{
    private FileInputStream fileInputStream;
    FileReader(FileInputStream fileInputStream) {
        this.fileInputStream = fileInputStream;
    }
    public void run() {
        try (BufferedReader bufferedReader =new BufferedReader(new InputStreamReader(fileInputStream))){
            String str ;
            while((str = bufferedReader.readLine()) != null){
                Main.queue.put(str);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class Entry{
    private Integer id;
    private float score;

    public Entry(Integer id, float score) {
        this.id = id;
        this.score = score;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

}