package com.github.bingoohuang.flume.more.source;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.bingoohuang.flume.more.source.ExecBlockSource.ExecBlockSourceConfigurationConstants.*;

/**
 * <p>
 * A {@link Source} implementation that executes a Unix process and turns each
 * line of text into an event.
 * </p>
 * <p>
 * This source runs a given Unix command on start up and expects that process to
 * continuously produce data on standard out (stderr ignored by default). Unless
 * told to restart, if the process exits for any reason, the source also exits and
 * will produce no further data. This means configurations such as <tt>cat [named pipe]</tt>
 * or <tt>tail -F [file]</tt> are going to produce the desired results where as
 * <tt>date</tt> will probably not - the former two commands produce streams of
 * data where as the latter produces a single event and exits.
 * </p>
 * <p>
 * The <tt>ExecBlockSource</tt> is meant for situations where one must integrate with
 * existing systems without modifying code. It is a compatibility gateway built
 * to allow simple, stop-gap integration and doesn't necessarily offer all of
 * the benefits or guarantees of native integration with Flume. If one has the
 * option of using the <tt>AvroSource</tt>, for instance, that would be greatly
 * preferred to this source as it (and similarly implemented sources) can
 * maintain the transactional guarantees that exec can not.
 * </p>
 * <p>
 * <i>Why doesn't <tt>ExecBlockSource</tt> offer transactional guarantees?</i>
 * </p>
 * <p>
 * The problem with <tt>ExecBlockSource</tt> and other asynchronous sources is that
 * the source can not guarantee that if there is a failure to put the event into
 * the {@link Channel} the client knows about it. As a for instance, one of the
 * most commonly requested features is the <tt>tail -F [file]</tt>-like use case
 * where an application writes to a log file on disk and Flume tails the file,
 * sending each block (with boundaryRegex) as an event. While this is possible, there's an obvious
 * problem; what happens if the channel fills up and Flume can't send an event?
 * Flume has no way of indicating to the application writing the log file that
 * it needs to retain the log or that the event hasn't been sent, for some
 * reason. If this doesn't make sense, you need only know this: <b>Your
 * application can never guarantee data has been received when using a
 * unidirectional asynchronous interface such as ExecBlockSource!</b> As an extension
 * of this warning - and to be completely clear - there is absolutely zero
 * guarantee of event delivery when using this source. You have been warned.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>command</tt></td>
 * <td>The command to execute</td>
 * <td>String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>restart</tt></td>
 * <td>Whether to restart the command when it exits</td>
 * <td>Boolean</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td><tt>restartThrottle</tt></td>
 * <td>How long in milliseconds to wait before restarting the command</td>
 * <td>Long</td>
 * <td>10000</td>
 * </tr>
 * <tr>
 * <td><tt>logStderr</tt></td>
 * <td>Whether to log or discard the standard error stream of the command</td>
 * <td>Boolean</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td><tt>batchSize</tt></td>
 * <td>The number of events to commit to channel at a time.</td>
 * <td>integer</td>
 * <td>20</td>
 * </tr>
 * <tr>
 * <td><tt>boundaryRegex</tt></td>
 * <td>The boundary regular expression to identify a log event which may contains multiple lines.</td>
 * <td>String</td>
 * <td>none</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class ExecBlockSource extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ExecBlockSource.class);

    private String shell;
    private String command;
    private SourceCounter sourceCounter;
    private ExecutorService executor;
    private Future<?> runnerFuture;
    private long restartThrottle;
    private boolean restart;
    private boolean logStderr;
    private Integer bufferCount;
    private long batchTimeout;
    private ExecRunnable runner;
    private Charset charset;

    final static String NEWLINE = System.getProperty("line.separator");
    private Pattern boundaryRegex;

    @Override
    public void start() {
        logger.info("Exec source starting with command:{}", command);

        executor = Executors.newSingleThreadExecutor();

        runner = new ExecRunnable(shell, command, getChannelProcessor(), sourceCounter,
                restart, restartThrottle, logStderr, bufferCount, batchTimeout, charset,
                boundaryRegex);

        // FIXME: Use a callback-like executor / future to signal us upon failure.
        runnerFuture = executor.submit(runner);

    /*
     * NB: This comes at the end rather than the beginning of the method because
     * it sets our state to running. We want to make sure the executor is alive
     * and well first.
     */
        sourceCounter.start();
        super.start();

        logger.debug("Exec source started");
    }

    @Override
    public void stop() {
        logger.info("Stopping exec source with command:{}", command);
        if (runner != null) {
            runner.setRestart(false);
            runner.kill();
        }

        if (runnerFuture != null) {
            logger.debug("Stopping exec runner");
            runnerFuture.cancel(true);
            logger.debug("Exec runner stopped");
        }
        executor.shutdown();

        while (!executor.isTerminated()) {
            logger.debug("Waiting for exec executor service to stop");
            try {
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for exec executor service "
                        + "to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }

        sourceCounter.stop();
        super.stop();

        logger.debug("Exec source with command:{} stopped. Metrics:{}", command,
                sourceCounter);
    }

    @Override
    public void configure(Context context) {
        command = context.getString("command");

        Preconditions.checkState(command != null, "The parameter command must be specified");

        restartThrottle = context.getLong(CONFIG_RESTART_THROTTLE, DEFAULT_RESTART_THROTTLE);
        restart = context.getBoolean(CONFIG_RESTART, DEFAULT_RESTART);
        logStderr = context.getBoolean(CONFIG_LOG_STDERR, DEFAULT_LOG_STDERR);
        bufferCount = context.getInteger(CONFIG_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        batchTimeout = context.getLong(CONFIG_BATCH_TIME_OUT, DEFAULT_BATCH_TIME_OUT);
        charset = Charset.forName(context.getString(CHARSET, DEFAULT_CHARSET));
        shell = context.getString(CONFIG_SHELL, null);
        String regex = context.getString(BOUNDARY_REGEX, null);
        boundaryRegex = StringUtils.isEmpty(regex) ? null : Pattern.compile(regex);

        if (sourceCounter == null) sourceCounter = new SourceCounter(getName());
    }

    private static class ExecRunnable implements Runnable {

        public ExecRunnable(String shell, String command, ChannelProcessor channelProcessor,
                            SourceCounter sourceCounter, boolean restart, long restartThrottle,
                            boolean logStderr, int bufferCount, long batchTimeout, Charset charset, Pattern boundaryRegex) {
            this.command = command;
            this.channelProcessor = channelProcessor;
            this.sourceCounter = sourceCounter;
            this.restartThrottle = restartThrottle;
            this.bufferCount = bufferCount;
            this.batchTimeout = batchTimeout;
            this.restart = restart;
            this.logStderr = logStderr;
            this.charset = charset;
            this.shell = shell;
            this.boundaryRegex = boundaryRegex;
        }

        private final String shell;
        private final String command;
        private final ChannelProcessor channelProcessor;
        private final SourceCounter sourceCounter;
        private volatile boolean restart;
        private final long restartThrottle;
        private final int bufferCount;
        private long batchTimeout;
        private final boolean logStderr;
        private final Charset charset;
        private Pattern boundaryRegex;
        private Process process = null;
        private SystemClock systemClock = new SystemClock();
        private Long lastPushToChannel = systemClock.currentTimeMillis();
        ScheduledExecutorService timedFlushService;
        ScheduledFuture<?> future;

        @Override
        public void run() {
            do {
                String exitCode;
                BufferedReader reader = null;

                final List<Event> eventList = new ArrayList<Event>();
                final StringBuilder block = new StringBuilder();

                timedFlushService = Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder().setNameFormat(
                                "timedFlushExecService" +
                                        Thread.currentThread().getId() + "-%d").build());
                try {
                    if (shell != null) {
                        String[] commandArgs = formulateShellCommand(shell, command);
                        process = Runtime.getRuntime().exec(commandArgs);
                    } else {
                        String[] commandArgs = command.split("\\s+");
                        process = new ProcessBuilder(commandArgs).start();
                    }
                    reader = new BufferedReader(
                            new InputStreamReader(process.getInputStream(), charset));

                    // StderrLogger dies as soon as the input stream is invalid
                    StderrReader stderrReader = new StderrReader(new BufferedReader(
                            new InputStreamReader(process.getErrorStream(), charset)), logStderr);
                    stderrReader.setName("StderrReader-[" + command + "]");
                    stderrReader.setDaemon(true);
                    stderrReader.start();

                    Runnable timeoutRun = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                boolean processTimeout = true;
                                flushBlockAndEvents(eventList, block, processTimeout);
                            } catch (Exception e) {
                                logger.error("Exception occurred when processing event batch", e);
                                if (e instanceof InterruptedException) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                    };
                    future = timedFlushService.scheduleWithFixedDelay(timeoutRun,
                            batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);

                    String line;
                    while ((line = reader.readLine()) != null) {
                        synchronized (eventList) {
                            if (boundaryRegex == null) {
                                sourceCounter.incrementEventReceivedCount();
                                eventList.add(EventBuilder.withBody(line.getBytes(charset)));
                            } else {
                                processBlockLog(eventList, block, line);
                            }
                            if (eventList.size() >= bufferCount || timeout()) {
                                flushEventBatch(eventList);
                            }
                        }
                    }

                    boolean processTimeout = false;
                    flushBlockAndEvents(eventList, block, processTimeout);
                } catch (Exception e) {
                    logger.error("Failed while running command: " + command, e);
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException ex) {
                            logger.error("Failed to close reader for exec source", ex);
                        }
                    }
                    exitCode = String.valueOf(kill());
                }
                if (restart) {
                    logger.info("Restarting in {}ms, exit code {}", restartThrottle, exitCode);
                    try {
                        Thread.sleep(restartThrottle);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    logger.info("Command [" + command + "] exited with " + exitCode);
                }
            } while (restart);
        }

        private void flushBlockAndEvents(List<Event> eventList, StringBuilder block, boolean processTimeout) {
            synchronized (eventList) {
                flushBlock(eventList, block);

                if (!eventList.isEmpty() && (!processTimeout || timeout())) {
                    flushEventBatch(eventList);
                }
            }
        }

        private void processBlockLog(List<Event> eventList, StringBuilder block, String line) {
            // Block break
            Matcher matcher = boundaryRegex.matcher(line);
            int start = 0;
            if (matcher.find()) {
                // Append the tail to last block before flushing it
                if (matcher.start() > 0) {
                    String tail = line.substring(start, matcher.start());
                    synchronized (block) {
                        if (start == 0 && block.length() > 0) block.append(NEWLINE);
                        block.append(tail);
                    }
                    // Reset the index of new block
                    start = matcher.start();
                }
                // Flush last block
                flushBlock(eventList, block);
            }
            synchronized (block) {
                if (block.length() > 0) block.append(NEWLINE);
                block.append(line.substring(start, line.length()));
            }
        }

        private void flushBlock(List<Event> eventList, StringBuilder block) {
            synchronized (block) {
                String blockStr = block.toString();
                if (StringUtils.isNotEmpty(blockStr)) {
                    sourceCounter.incrementEventReceivedCount();
                    eventList.add(EventBuilder.withBody(blockStr, charset));
                    block.delete(0, block.length());
                }
            }
        }

        private void flushEventBatch(List<Event> eventList) {
            channelProcessor.processEventBatch(eventList);
            sourceCounter.addToEventAcceptedCount(eventList.size());
            eventList.clear();
            lastPushToChannel = systemClock.currentTimeMillis();
        }

        private boolean timeout() {
            return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
        }

        private static String[] formulateShellCommand(String shell, String command) {
            String[] shellArgs = shell.split("\\s+");
            String[] result = new String[shellArgs.length + 1];
            System.arraycopy(shellArgs, 0, result, 0, shellArgs.length);
            result[shellArgs.length] = command;
            return result;
        }

        public int kill() {
            if (process == null) return Integer.MIN_VALUE / 2;

            synchronized (process) {
                process.destroy();

                try {
                    int exitValue = process.waitFor();

                    // Stop the Thread that flushes periodically
                    if (future != null) future.cancel(true);

                    if (timedFlushService != null) {
                        timedFlushService.shutdown();
                        while (!timedFlushService.isTerminated()) {
                            try {
                                timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                                logger.debug("Interrupted while waiting for exec executor service "
                                        + "to stop. Just exiting.");
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                    return exitValue;
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            return Integer.MIN_VALUE;
        }

        public void setRestart(boolean restart) {
            this.restart = restart;
        }
    }

    private static class StderrReader extends Thread {
        private BufferedReader input;
        private boolean logStderr;

        protected StderrReader(BufferedReader input, boolean logStderr) {
            this.input = input;
            this.logStderr = logStderr;
        }

        @Override
        public void run() {
            try {
                int i = 0;
                String line = null;
                while ((line = input.readLine()) != null) {
                    if (logStderr) {
                        // There is no need to read 'line' with a charset
                        // as we do not to propagate it.
                        // It is in UTF-16 and would be printed in UTF-8 format.
                        logger.info("StderrLogger[{}] = '{}'", ++i, line);
                    }
                }
            } catch (IOException e) {
                logger.info("StderrLogger exiting", e);
            } finally {
                try {
                    if (input != null) {
                        input.close();
                    }
                } catch (IOException ex) {
                    logger.error("Failed to close stderr reader for exec source", ex);
                }
            }
        }
    }


    public interface ExecBlockSourceConfigurationConstants {

        /**
         * Should the exec'ed command restarted if it dies: : default false
         */
        String CONFIG_RESTART = "restart";
        boolean DEFAULT_RESTART = false;

        /**
         * Amount of time to wait before attempting a restart: : default 10000 ms
         */
        String CONFIG_RESTART_THROTTLE = "restartThrottle";
        long DEFAULT_RESTART_THROTTLE = 10000L;

        /**
         * Should stderr from the command be logged: default false
         */
        String CONFIG_LOG_STDERR = "logStdErr";
        boolean DEFAULT_LOG_STDERR = false;

        /**
         * Number of lines to read at a time
         */
        String CONFIG_BATCH_SIZE = "batchSize";
        int DEFAULT_BATCH_SIZE = 20;

        /**
         * Amount of time to wait, if the buffer size was not reached, before
         * to data is pushed downstream: : default 3000 ms
         */
        String CONFIG_BATCH_TIME_OUT = "batchTimeout";
        long DEFAULT_BATCH_TIME_OUT = 3000l;

        /**
         * Charset for reading input
         */
        String CHARSET = "charset";
        String DEFAULT_CHARSET = "UTF-8";

        /**
         * Optional shell/command processor used to run command
         */
        String CONFIG_SHELL = "shell";

        /**
         * Boundary regular pattern to separate log events.
         */
        String BOUNDARY_REGEX = "boundaryRegex";
    }
}