package ccjz_rgzn_kafka;

/**
 * Package: ccjz_rgzn_kafka
 * Description：
 * Author: Jermyn
 * Date: Created in 2022/4/29 0029 13:08
 * Version: 0.0.1
 */

import java.util.concurrent.*;

public class CallableDemo { public static void main(String[] args) throws ExecutionException, InterruptedException {

//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//
//                //执行相应的业务逻辑
//                int i = 0;
//                while(i<100){
//                    System.out.println(i);
//                    i++;
//                }
////                return "ok";
//            }
//        }).start();


//        Callable<String> callable = new Callable<String>() {
//            @Override
//            public String call() throws Exception {
//
//                return null;
//            }
//        };

        System.out.println("准备去开启一个线程工作......");

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<String> future = executorService.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                //执行相应的业务逻辑
                int i = 0;
                while (i < 20) {
                    System.out.println(i);
                    i++;
                    Thread.sleep(1000);
                }
                return "ok";
            }
        });

//        System.out.println("我要去获取线程的执行结果并返回");
//        String s = future.get();
//        System.out.println("拿到了吗？");

        boolean fetched = false;

        System.out.println("我要去获取线程的执行结果并返回,最多等500ms");
        try {
            String s = future.get(500, TimeUnit.MILLISECONDS);
            System.out.println(s);
        } catch (Exception e) {
            System.out.println("没有拿到");
        }

        if (!fetched) {
            System.out.println("我要去继续获取线程的返回结果，一直拿到为止");
            try {
                String s = future.get();
                System.out.println(s);
                executorService.shutdown();
            } catch (Exception e) {
                System.out.println("没有拿到");
            }
        }
    }


}