/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */
package edu.ucsc.dbtune.util;

import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.spi.core.Printer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Threads {
    private Threads(){}

    /**
     * creates a {@link ThreadFactory daemon thread factory} object.
     * @param name thread name
     * @return a daemon thread factory.
     */
    public static ThreadFactory daemonThreadFactory(final String name) {
        return threadFactory(name, true);
    }

    /**
     * creates a {@link ThreadFactory daemon thread factory} object.
     * @param name thread name
     * @param daemon flag that signals whether the produced threads are daemon threads.
     * @return a thread factory.
     */
    public static ThreadFactory threadFactory(final String name, final boolean daemon) {
        return new ThreadFactory() {
            private int nextId = 0;
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, name + "-" + (nextId++));
                thread.setDaemon(daemon);
                return thread;
            }
        };
    }

    /**
     * Creates an executor service that produces daemon threads.
     *
     * @param name name of process.
     * @param echo printer to screen.
     * @return {@link ExecutorService daemon thread executor service}.
     */
    public  static ExecutorService daemonThreadPerCpuExecutor(String name, Printer echo){
        return threadPerCpuExecutor(name, true, echo);
    }

    /**
     * Creates an executor service that produces threads.
     *
     * @param name name of process.
     * @param echo printer to screen
     * @return {@link ExecutorService thread executor service}.
     */
    public static ExecutorService explicitThreadPerCpuExecutor(String name, Printer echo){
        return threadPerCpuExecutor(name, false, echo);
    }

    public static ExecutorService explicitThreadPerCpuExecutor(String name){
        return explicitThreadPerCpuExecutor(name, Console.streaming());
    }

    /**
     * Creates an executor service that produces threads, which are either daemon or normal
     * threads.
     *
     * @param name name of process.
     * @param daemon flag that signals whether the produced threads a daemon threads.
     * @param echo prints things to screen.
     * @return {@link ExecutorService thread executor service}.
     */
    public static ExecutorService threadPerCpuExecutor(String name, boolean daemon, Printer echo) {
        return fixedThreadsExecutor(name, Runtime.getRuntime().availableProcessors(), daemon, echo);
    }


    /**
     * Creates an executor service that produces a fixed number of threads.
     *
     * @param name name of process.
     * @param count the number of threads to be created.
     * @param daemon flag that signals whether the produced threads are daemon threads.
     * @param echo results printer (on screen)
     * @return {@link ExecutorService thread executor service}.
     */
    public static ExecutorService fixedThreadsExecutor(String name, int count, boolean daemon, final Printer echo) {
        ThreadFactory threadFactory = daemon ? daemonThreadFactory(name) : threadFactory(name, daemon);

        return new ThreadPoolExecutor(count, count, 10, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(Integer.MAX_VALUE), threadFactory) {
            @Override protected void afterExecute(Runnable runnable, Throwable throwable) {
                if (throwable != null) {
                    echo.error("Unexpected failure from " + runnable, throwable);
                }
            }
        };
    }
}
