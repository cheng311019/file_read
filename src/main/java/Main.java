
import java.io.*;
import java.util.TreeMap;
import java.util.concurrent.*;
/**
 * @author chenggang
 * */
public class Main {
    private static volatile Boolean RUN = true;
    private static final String FILE_DIR = "src/main/resources";
    //读取文件的线程组
    private static ExecutorService executorService =Executors.newFixedThreadPool(10);
    //阻塞队列，读取文件线程将每一行记录写入队列中
    private static LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
    //存储数据的map
    private static TreeMap<Integer, TextRow> map = new TreeMap<>();
    public static void main(String[] args) {
        //加载文件
        File[] files = loadFiles(args[0]);
        CountDownLatch loadFileLatch = new CountDownLatch(files.length);
        CountDownLatch printLatch = new CountDownLatch(1);
        for (File item :files) {
            //将文件交由线程池处理
            executorService.execute(() ->{
                try (FileInputStream fileInputStream = new FileInputStream(item);
                     InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                     BufferedReader bufferedReader =new BufferedReader(inputStreamReader))
                {
                    String str ;
                    while((str = bufferedReader.readLine()) != null){
                        Main.queue.put(str);
                    }
                    loadFileLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        //消费线程，用于处理queue中的数据
        new Thread(() -> {
            while (RUN || Main.queue.size() != 0) {
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
                    TextRow textRow = Main.map.get(groupId);
                    if (textRow.getScore() > quota) {
                        textRow.setId(id);
                        textRow.setScore(quota);
                    }
                } else {
                    Main.map.put(groupId, new TextRow(groupId, id, quota));
                }
            }

            printLatch.countDown();
        }).start();
        try {
            loadFileLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            RUN =false;
            executorService.shutdownNow();
        }
        try {
            printLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        map.forEach((groupId, entry) -> System.out.println(entry.toString()));
    }

    private static File[] loadFiles(String fileDir){
        //读取系统文件参数
        File file = new File(fileDir == null ? FILE_DIR : fileDir);
        //获取所有文件
        File[] tempList = file.listFiles();
        if(tempList == null ){
            throw new RuntimeException("file not found");
        }
        return tempList;
    }
}