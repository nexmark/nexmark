package com.github.nexmark.flink.metrics;

import org.apache.flink.state.forst.ForStNativeMetricOptions;
import org.apache.flink.state.forst.ForStOptionsFactory;
import org.apache.flink.state.forst.ForStProperty;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.DBOptions;
import org.forstdb.TickerType;

import java.util.Arrays;
import java.util.Collection;

/** Exposes only data-block cache hits and misses from ForSt's native statistics. */
public final class ForStDataBlockMetricsFactory implements ForStOptionsFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public DBOptions createDBOptions(
            DBOptions options, Collection<AutoCloseable> handlesToClose) {
        return options;
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions options, Collection<AutoCloseable> handlesToClose) {
        return options;
    }

    @Override
    public ForStNativeMetricOptions createNativeMetricsOptions(ForStNativeMetricOptions options) {
        // Flink decides whether to create the native Statistics object before invoking this hook.
        // Keep at least one built-in statistics option enabled in the Flink configuration.
        if (!options.isStatisticsEnabled()) {
            throw new IllegalStateException(
                    "Enable state.backend.forst.metrics.block-cache-hit so ForSt creates "
                            + "native statistics; this factory reports data hits and misses only.");
        }
        return new DataOnlyOptions(options);
    }

    private static final class DataOnlyOptions extends ForStNativeMetricOptions {
        private static final long serialVersionUID = 1L;

        private final ForStNativeMetricOptions original;

        private DataOnlyOptions(ForStNativeMetricOptions original) {
            this.original = original;
        }

        @Override
        public Collection<TickerType> getMonitorTickerTypes() {
            return Arrays.asList(TickerType.BLOCK_CACHE_DATA_HIT, TickerType.BLOCK_CACHE_DATA_MISS);
        }

        @Override
        public Collection<ForStProperty> getProperties() {
            return original.getProperties();
        }

        @Override
        public boolean isColumnFamilyAsVariable() {
            return original.isColumnFamilyAsVariable();
        }

        @Override
        public boolean isStatisticsEnabled() {
            return true;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }
    }
}
