package com.cqz.component.common.concurrent;

import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LogService {
    private final BlockingQueue<String> queue;
    private final LoggerThread loggerThread;
    private boolean isShutDown;
    private int reservations;

    public LogService(PrintWriter writer) {
        this.queue = new LinkedBlockingQueue<>();
        this.loggerThread = new LoggerThread(writer);
    }

    public void start(){
        loggerThread.start();
    }

    public void stop(){
        synchronized (this){
            isShutDown = true;
        }
        loggerThread.interrupt();
    }

    public void log(String msg) throws InterruptedException {
        synchronized (this){
            if (isShutDown)
                throw new IllegalStateException("thread is shutdown");
            ++reservations;
        }
        queue.put(msg);
    }

    public class LoggerThread extends Thread {
        private final PrintWriter writer;

        public LoggerThread(PrintWriter writer) {
            this.writer = writer;
        }

        @Override
        public void run() {
            try{
                while (true){
                    synchronized (LogService.this){
                        if (isShutDown && reservations==0)
                            break;
                    }
                    String msg = queue.take();
                    synchronized (LogService.this){
                        --reservations;
                    }
                    writer.println(msg);
                }
            }catch (InterruptedException ignore){
            }finally {
                writer.close();
            }


        }
    }

}
