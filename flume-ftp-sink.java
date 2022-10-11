package com.jzy.flume;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.formatter.output.PathManager;
import org.apache.flume.formatter.output.PathManagerFactory;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class FlumeFtpSink extends AbstractSink  implements Configurable, BatchSizeSupported {

    private static final Logger logger = LoggerFactory.getLogger(FlumeFtpSink.class);

    private String host;
    private int port;
    private String user;
    private String password;
    private String ftpDirPath;
    private String realDirPath;
    private String suffix;
    private String tempSuffix;
    private String prefix;
    private String realprefix;
    private long rollInterval;
    private ScheduledExecutorService rollService;
    private volatile boolean shouldRotate;
    private PathManager pathController;

    private FTPClient ftpClient;

    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int INTERVAL_SECONDS = 5;
    private int batchSize;

    private static final String WORK_DIR_PATH = "/tmp/flume/";
    private final File workDir = new File(WORK_DIR_PATH);

    private SinkCounter sinkCounter;
    private TimeZone timeZone;

    private boolean needRounding = false;
    private int roundUnit = Calendar.SECOND;
    private int roundValue = 1;
    private boolean useLocalTime = false;


    @Override
    public void configure(Context context) {
        host = context.getString("host");
        port = context.getInteger("port", 21);
        user = context.getString("user");
        password = context.getString("password");
        ftpDirPath = context.getString("ftpDirPath", "/");
        suffix = context.getString("suffix", ".dat");
        tempSuffix = context.getString("tempSuffix", ".tmp");
        prefix = context.getString("prefix", "DATA-");
        String rollInterval = context.getString("rollInterval", "30");
        String pathManagerType = context.getString("pathManager", "DEFAULT");
        this.rollInterval = Long.parseLong(rollInterval);
        Context pathManagerContext =
                new Context(context.getSubProperties(
                        PathManager.CTX_PREFIX));
        pathController = PathManagerFactory.getInstance(pathManagerType, pathManagerContext);

        batchSize = context.getInteger("batchSize", DEFAULT_BATCH_SIZE);

        needRounding = context.getBoolean("round", false);

        String tzName = context.getString("timeZone");
        timeZone = tzName == null ? null : TimeZone.getTimeZone(tzName);

        if (needRounding) {
            String unit = context.getString("roundUnit", "second");
            if (unit.equalsIgnoreCase("hour")) {
                this.roundUnit = Calendar.HOUR_OF_DAY;
            } else if (unit.equalsIgnoreCase("minute")) {
                this.roundUnit = Calendar.MINUTE;
            } else if (unit.equalsIgnoreCase("second")) {
                this.roundUnit = Calendar.SECOND;
            } else {
                logger.warn("Rounding unit is not valid, please set one of" +
                        "minute, hour, or second. Rounding will be disabled");
                needRounding = false;
            }
            this.roundValue = context.getInteger("roundValue", 1);
            if (roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE) {
                Preconditions.checkArgument(roundValue > 0 && roundValue <= 60,
                        "Round value" +
                                "must be > 0 and <= 60");
            } else if (roundUnit == Calendar.HOUR_OF_DAY) {
                Preconditions.checkArgument(roundValue > 0 && roundValue <= 24,
                        "Round value" +
                                "must be > 0 and <= 24");
            }
        }

        ftpClient = new FTPClient();

        if(!workDir.exists()) {
            workDir.mkdir();
        }
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }

        logger.info("Loading FtpSink config: host={}, port={}, user={}, password={}", host, port, user, password);
    }

    @Override
    public synchronized void start() {
        sinkCounter.start();
        connectServer();
        super.start();
        if (rollInterval > 0) {

            rollService = Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder().setNameFormat(
                            "rollingFileSink-roller-" +
                                    Thread.currentThread().getId() + "-%d").build());

            /*
             * Every N seconds, mark that it's time to rotate. We purposefully do NOT
             * touch anything other than the indicator flag to avoid error handling
             * issues (e.g. IO exceptions occuring in two different threads.
             * Resist the urge to actually perform rotation in a separate thread!
             */
            rollService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    logger.debug("Marking time to rotate file {}",
                            pathController.getCurrentFile());
                    shouldRotate = true;
                }

            }, rollInterval, rollInterval, TimeUnit.SECONDS);
        } else {
            logger.info("RollInterval is not valid, file rolling will not happen.");
        }
        logger.info("RollingFileSink {} started.", getName());
    }

    @Override
    public synchronized void stop() {
        disConnectServer();
        sinkCounter.stop();
        super.stop();
        if (rollInterval > 0) {
            rollService.shutdown();

            while (!rollService.isTerminated()) {
                try {
                    rollService.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.debug("Interrupted while waiting for roll service to stop. " +
                            "Please report this.", e);
                }
            }
        }
        logger.info("RollingFile sink {} stopped. Event metrics: {}",
                getName(), sinkCounter);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        try {
            transaction.begin();
            verifyConnection();

            List<Event> batch = new ArrayList<>();

            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }
                realDirPath = BucketPath.escapeString(ftpDirPath, event.getHeaders(),
                        timeZone, needRounding, roundUnit, roundValue, useLocalTime);
                realprefix = BucketPath.escapeString(prefix, event.getHeaders(),
                        timeZone, needRounding, roundUnit, roundValue, useLocalTime);
                batch.add(event);
            }

            if (batch.size() == 0) {
                sinkCounter.incrementBatchEmptyCount();
                status = Status.BACKOFF;
            } else {
                if (batch.size() < batchSize) {
                    sinkCounter.incrementBatchUnderflowCount();
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }
                sinkCounter.addToEventDrainAttemptCount(batch.size());
                dealEventList(batch);
            }

            sinkCounter.addToEventDrainSuccessCount(batch.size());
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            sinkCounter.incrementEventWriteOrChannelFail(ex);
            transaction.rollback();
            throw new EventDeliveryException("Failed to send events", ex);
        } finally {
            transaction.commit();
            transaction.close();
        }

        return status;
    }

    private void connectServer() {
        for (int i = 5; ; i+=INTERVAL_SECONDS) {
            try {
                ftpClient.connect(host, port);
                ftpClient.login(user, password);

                int reply = ftpClient.getReplyCode();
                if (!FTPReply.isPositiveCompletion(reply)) {
                    logger.error("ftp reply failed, reply-code:" + reply);
                    ftpClient.disconnect();
                    throw new IOException("ftp reply failed, reply-code:" + reply);
                }

                ftpClient.enterLocalPassiveMode();
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                boolean dirFlag = ftpClient.changeWorkingDirectory("/");
                if (!dirFlag) {
                    logger.error("change ftp dir failed, dir:" + ftpDirPath);
                    throw new IOException("change ftp dir failed, dir:" + ftpDirPath);
                }
                logger.info("ftp server is connected");
                sinkCounter.incrementConnectionCreatedCount();
                break;
            } catch (IOException e) {
                sinkCounter.incrementConnectionFailedCount();
                logger.error("Can't connect FtpServer, {}:{}", host, port);
                logger.error(e.getMessage(), e);
                logger.info("{}s will retry to connect", i);
                try {
                    Thread.sleep(i * 1000L);
                } catch (InterruptedException ex) {
                    logger.error(ex.getMessage(), ex);
                }
            }
        }
    }

    private void disConnectServer() {
        try {
            ftpClient.logout();
            ftpClient.disconnect();
            sinkCounter.incrementConnectionClosedCount();
        } catch (IOException e) {
            sinkCounter.incrementConnectionFailedCount();
            logger.error(e.getMessage(), e);
        }

    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

    private void verifyConnection() {
        if (!ftpClient.isConnected()) {
            connectServer();
        }
    }

    private void renameFtpFile(String srcFile, String targetFile) throws Exception {
        try {
            boolean renameFlag = ftpClient.rename(srcFile, targetFile);
            if (!renameFlag) {
                int code = ftpClient.getReplyCode();
                logger.error("ftp-file rename failed, ftp-code=" + code);
                throw new Exception("ftp-file rename failed, ftp-code=" + code);
            }
            logger.info("ftp rename success, file={}", targetFile);
        } catch (IOException e) {
            logger.error("ftp-file rename exception:" + e.getMessage());
            logger.error(e.getMessage(), e);
            throw e;
        }
    }


    private void dealEventList(List<Event> eventList) throws Exception {
        if (eventList.size() == 0) {
            return;
        }

        //logger.info("deal data numbers: {}", eventList.size());
        String filename = realprefix + System.currentTimeMillis() + suffix + tempSuffix;
        //logger.info("filename: {}", filename);
        File localFile = new File(WORK_DIR_PATH + filename);
        if (!localFile.exists()) {
            boolean newFileFlag = localFile.createNewFile();
            if (!newFileFlag) {
                throw new Exception("local temp file create failed");
            }
        }

        FileWriter fw = new FileWriter(localFile);
        for (int i = 0; i < eventList.size(); i ++) {
            if (i == eventList.size() - 1) {
                fw.write(new String(eventList.get(i).getBody()));
            } else {
                fw.write(new String(eventList.get(i).getBody()) + "\n");
            }
        }

        fw.flush();
        fw.close();

        // upload ftp
        FileInputStream fis = new FileInputStream(localFile);
        logger.info("ftpDirPath:{}", realDirPath);
        ftpClient.changeWorkingDirectory("/");
        boolean dirFlag = ftpClient.changeWorkingDirectory(realDirPath);
        if (!dirFlag) {
            makeDir(ftpClient,realDirPath);
        }
        //logger.info("ftpDirPath:{}", ftpClient.printWorkingDirectory());
        boolean storeFileFlag = ftpClient.storeFile(filename, fis);
        fis.close();
        if (!storeFileFlag) {
            int code = ftpClient.getReplyCode();
            logger.error("file upload to ftp failed, ftp-code={},  filename={}", code, filename);
            throw new Exception("file upload to ftp failed, ftp-code=" + code + ", filename=" + filename);
        }

        logger.info("ftp upload success!, filename={}", filename);
        fis.close();

        // rename ftp file
        String ftpFilename = filename.substring(0, filename.indexOf(tempSuffix));
        renameFtpFile(filename, ftpFilename);

        boolean deleteFlag = localFile.delete();
        if (!deleteFlag) {
            logger.warn("local file deleted failed, file={}", filename);
        }
    }

    /**
     * ftp创建目录——ftpClient只支持一级一级创建
     * @param ftp
     * @param path
     * @return
     * @throws IOException
     */
    boolean makeDir(FTPClient ftp,String path) throws IOException {
        //分割
        String[] paths = path.split("/");
        //创建成功标识
        boolean isMakeSucess=false;
        //遍历每一级路径
        for (String str : paths) {
            if (StringUtils.isBlank(str)) {
                continue;
            }
            //切换目录，根据切换是否成功判断子目录是否存在
            boolean changeSuccess = ftp.changeWorkingDirectory(str);
            //该级路径不存在就创建并切换
            if (!changeSuccess) {
                isMakeSucess = ftp.makeDirectory(str);
                if (!isMakeSucess) {
                    logger.error("make ftp dir failed, dir:" + str);
                    throw new IOException("make ftp dir failed, dir:" + str);
                }
                logger.info("make ftp dir success, dir:" + str);
                isMakeSucess = ftp.changeWorkingDirectory(str);
                if (!isMakeSucess) {
                    logger.error("change ftp dir failed, dir:" + str);
                    throw new IOException("change ftp dir failed, dir:" + str);
                }
                logger.info("change ftp dir success, dir:" + str);
            }
        }
        return isMakeSucess;
    }

}
