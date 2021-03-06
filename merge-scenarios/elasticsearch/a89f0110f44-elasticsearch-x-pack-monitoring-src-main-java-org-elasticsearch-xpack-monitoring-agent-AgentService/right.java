package org.elasticsearch.xpack.monitoring.agent;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.agent.collector.Collector;
import org.elasticsearch.xpack.monitoring.agent.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.xpack.monitoring.agent.exporter.ExportException;
import org.elasticsearch.xpack.monitoring.agent.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.agent.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.agent.exporter.MonitoringDoc;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class AgentService extends AbstractLifecycleComponent {

    private volatile ExportingWorker exportingWorker;

    private volatile Thread workerThread;

    private volatile long samplingIntervalMillis;

    private final Collection<Collector> collectors;

    private final String[] settingsCollectors;

    private final Exporters exporters;

    public AgentService(Settings settings, ClusterSettings clusterSettings, Set<Collector> collectors, Exporters exporters) {
        super(settings);
        this.samplingIntervalMillis = MonitoringSettings.INTERVAL.get(settings).millis();
        this.settingsCollectors = MonitoringSettings.COLLECTORS.get(settings).toArray(new String[0]);
        this.collectors = Collections.unmodifiableSet(filterCollectors(collectors, settingsCollectors));
        this.exporters = exporters;
        clusterSettings.addSettingsUpdateConsumer(MonitoringSettings.INTERVAL, this::setInterval);
    }

    private void setInterval(TimeValue interval) {
        this.samplingIntervalMillis = interval.millis();
        applyIntervalSettings();
    }

    protected Set<Collector> filterCollectors(Set<Collector> collectors, String[] filters) {
        if (CollectionUtils.isEmpty(filters)) {
            return collectors;
        }
        Set<Collector> list = new HashSet<>();
        for (Collector collector : collectors) {
            if (Regex.simpleMatch(filters, collector.name().toLowerCase(Locale.ROOT))) {
                list.add(collector);
            } else if (collector instanceof ClusterStatsCollector) {
                list.add(collector);
            }
        }
        return list;
    }

    protected void applyIntervalSettings() {
        if (samplingIntervalMillis <= 0) {
            logger.info("data sampling is disabled due to interval settings [{}]", samplingIntervalMillis);
            if (workerThread != null) {
                exportingWorker.closed = true;
                exportingWorker = null;
                workerThread = null;
            }
        } else if (workerThread == null || !workerThread.isAlive()) {
            exportingWorker = new ExportingWorker();
            workerThread = new Thread(exportingWorker, EsExecutors.threadName(settings, "monitoring.exporters"));
            workerThread.setDaemon(true);
            workerThread.start();
        }
    }

    public void stopCollection() {
        final ExportingWorker worker = this.exportingWorker;
        if (worker != null) {
            worker.stopCollecting();
        }
    }

    public void startCollection() {
        final ExportingWorker worker = this.exportingWorker;
        if (worker != null) {
            worker.collecting = true;
        }
    }

    @Override
    protected void doStart() {
        logger.debug("monitoring service started");
        for (Collector collector : collectors) {
            collector.start();
        }
        exporters.start();
        applyIntervalSettings();
    }

    @Override
    protected void doStop() {
        if (workerThread != null && workerThread.isAlive()) {
            exportingWorker.closed = true;
            workerThread.interrupt();
            try {
                workerThread.join(60000);
            } catch (InterruptedException e) {
            }
        }
        for (Collector collector : collectors) {
            collector.stop();
        }
        exporters.stop();
    }

    @Override
    protected void doClose() {
        for (Collector collector : collectors) {
            collector.close();
        }
        for (Exporter exporter : exporters) {
            try {
                exporter.close();
            } catch (Exception e) {
                logger.error("failed to close exporter [{}]", e, exporter.name());
            }
        }
    }

    public TimeValue getSamplingInterval() {
        return TimeValue.timeValueMillis(samplingIntervalMillis);
    }

    public String[] collectors() {
        return settingsCollectors;
    }

    class ExportingWorker implements Runnable {

        volatile boolean closed = false;

        volatile boolean collecting = true;

        final ReleasableLock collectionLock = new ReleasableLock(new ReentrantLock(false));

        @Override
        public void run() {
            while (!closed) {
                try {
                    Thread.sleep(samplingIntervalMillis);
                    if (closed) {
                        continue;
                    }
                    try (Releasable ignore = collectionLock.acquire()) {
                        Collection<MonitoringDoc> docs = collect();
                        if ((docs.isEmpty() == false) && (closed == false)) {
                            exporters.export(docs);
                        }
                    }
                } catch (ExportException e) {
                    logger.error("exception when exporting documents", e);
                } catch (InterruptedException e) {
                    logger.trace("interrupted");
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    logger.error("background thread had an uncaught exception", e);
                }
            }
            logger.debug("worker shutdown");
        }

        public void stopCollecting() {
            collecting = false;
            collectionLock.acquire().close();
        }

        private Collection<MonitoringDoc> collect() {
            if (logger.isTraceEnabled()) {
                logger.trace("collecting data - collectors [{}]", Strings.collectionToCommaDelimitedString(collectors));
            }
            Collection<MonitoringDoc> docs = new ArrayList<>();
            for (Collector collector : collectors) {
                if (collecting) {
                    Collection<MonitoringDoc> result = collector.collect();
                    if (result != null) {
                        logger.trace("adding [{}] collected docs from [{}] collector", result.size(), collector.name());
                        docs.addAll(result);
                    } else {
                        logger.trace("skipping collected docs from [{}] collector", collector.name());
                    }
                }
                if (closed) {
                    break;
                }
            }
            return docs;
        }
    }
}