/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.listener;

import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.DEFAULT_TOP_N_QUERY_MAP;
import static org.opensearch.plugin.insights.settings.QueryCategorizationSettings.SEARCH_QUERY_METRICS_ENABLED_SETTING;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_EXCLUDED_INDICES;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_GROUPING_FIELD_NAME;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_GROUPING_FIELD_TYPE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_GROUP_BY;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNEnabledSetting;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNSizeSetting;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNWindowSizeSetting;

// Added
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
// Added
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.transport.client.Client;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import reactor.util.annotation.NonNull;

/**
 * The listener for query insights services.
 * It forwards query-related data to the appropriate query insights stores,
 * either for each request or for each phase.
 */
public final class QueryInsightsListener extends SearchRequestOperationsListener {

    private static final Logger log = LogManager.getLogger(QueryInsightsListener.class);

    private final QueryInsightsService queryInsightsService;
    private final ClusterService clusterService;
    private final Client client;
    private boolean groupingFieldNameEnabled;
    private boolean groupingFieldTypeEnabled;
    private final QueryShapeGenerator queryShapeGenerator;
    private Set<Pattern> excludedIndicesPattern;


    // Added:
    // For Query Export
    private final String queryExportFilePath;

    // For writing to the same file to aggregate the workload data:
    // private final String queryExportFilePath = "fullQueryMetrics.json";
    
    private final List<SearchQueryRecord> queryBuffer = new ArrayList<>();
    private static final int BATCH_SIZE = 1;
    
    // Cache for document counts by index key
    private final Map<String, Long> docCountCache = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Constructor for QueryInsightsListener
     *
     * @param clusterService       The Node's cluster service.
     * @param queryInsightsService The topQueriesByLatencyService associated with this listener
     */
    @Inject
    public QueryInsightsListener(final ClusterService clusterService, final QueryInsightsService queryInsightsService) {
        this(clusterService, queryInsightsService, null, false);
        groupingFieldNameEnabled = false;
        groupingFieldTypeEnabled = false;
    }

    /**
     * Constructor for QueryInsightsListener
     *
     * @param clusterService       The Node's cluster service.
     * @param queryInsightsService The topQueriesByLatencyService associated with this listener
     * @param initiallyEnabled Is the listener initially enabled/disabled
     */
    public QueryInsightsListener(
        final ClusterService clusterService,
        final QueryInsightsService queryInsightsService,
        final Client client,
        boolean initiallyEnabled
    ) {
        super(initiallyEnabled);
        this.clusterService = clusterService;
        this.queryInsightsService = queryInsightsService;
        this.client = client;
        this.queryShapeGenerator = new QueryShapeGenerator(clusterService);
        queryInsightsService.setQueryShapeGenerator(queryShapeGenerator);

        // Generate unique filename with readable timestamp
        this.queryExportFilePath = "BenchmarkOutputs/queryMetrics_" +
            DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").withZone(ZoneOffset.UTC).format(Instant.now()) + ".json";

        // Setting endpoints set up for top n queries, including enabling top n queries, window size, and top n size
        // Expected metricTypes are Latency, CPU, and Memory.
        for (MetricType type : MetricType.allMetricTypes()) {
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(getTopNEnabledSetting(type), v -> this.setEnableTopQueries(type, v));
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    getTopNSizeSetting(type),
                    v -> this.queryInsightsService.setTopNSize(type, v),
                    v -> this.queryInsightsService.validateTopNSize(type, v)
                );
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    getTopNWindowSizeSetting(type),
                    v -> this.queryInsightsService.setWindowSize(type, v),
                    v -> this.queryInsightsService.validateWindowSize(type, v)
                );

            this.setEnableTopQueries(type, clusterService.getClusterSettings().get(getTopNEnabledSetting(type)));
            this.queryInsightsService.validateTopNSize(type, clusterService.getClusterSettings().get(getTopNSizeSetting(type)));
            this.queryInsightsService.setTopNSize(type, clusterService.getClusterSettings().get(getTopNSizeSetting(type)));
            this.queryInsightsService.validateWindowSize(type, clusterService.getClusterSettings().get(getTopNWindowSizeSetting(type)));
            this.queryInsightsService.setWindowSize(type, clusterService.getClusterSettings().get(getTopNWindowSizeSetting(type)));
        }

        // Settings endpoints set for grouping top n queries
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_QUERIES_GROUP_BY,
                v -> this.queryInsightsService.setGrouping(v),
                v -> this.queryInsightsService.validateGrouping(v)
            );
        this.queryInsightsService.validateGrouping(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUP_BY));
        this.queryInsightsService.setGrouping(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUP_BY));

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N,
                v -> this.queryInsightsService.setMaximumGroups(v),
                v -> this.queryInsightsService.validateMaximumGroups(v)
            );
        this.queryInsightsService.validateMaximumGroups(clusterService.getClusterSettings().get(TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N));
        this.queryInsightsService.setMaximumGroups(clusterService.getClusterSettings().get(TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N));

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                QueryInsightsSettings.TOP_N_QUERIES_EXCLUDED_INDICES,
                this::setExcludedIndices,
                this::validateExcludedIndices
            );
        validateExcludedIndices(clusterService.getClusterSettings().get(TOP_N_QUERIES_EXCLUDED_INDICES));
        setExcludedIndices(clusterService.getClusterSettings().get(TOP_N_QUERIES_EXCLUDED_INDICES));

        // Internal settings for grouping attributes
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TOP_N_QUERIES_GROUPING_FIELD_NAME, this::setGroupingFieldNameEnabled);
        setGroupingFieldNameEnabled(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUPING_FIELD_NAME));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(TOP_N_QUERIES_GROUPING_FIELD_TYPE, this::setGroupingFieldTypeEnabled);
        setGroupingFieldTypeEnabled(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUPING_FIELD_TYPE));

        // Settings endpoints set for search query metrics
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SEARCH_QUERY_METRICS_ENABLED_SETTING, this::setSearchQueryMetricsEnabled);
        setSearchQueryMetricsEnabled(clusterService.getClusterSettings().get(SEARCH_QUERY_METRICS_ENABLED_SETTING));
    }

    private void setExcludedIndices(List<String> excludedIndices) {
        this.excludedIndicesPattern = excludedIndices.stream()
            .map(index -> index.contains("*") ? index.replace("*", ".*") : index)
            .map(Pattern::compile)
            .collect(Collectors.toSet());
    }

    /**
     * Enable or disable top queries insights collection for {@link MetricType}.
     * This function will enable or disable the corresponding listeners
     * and query insights services.
     *
     * @param metricType {@link MetricType}
     * @param isCurrentMetricEnabled boolean
     */
    public void setEnableTopQueries(final MetricType metricType, final boolean isCurrentMetricEnabled) {
        this.queryInsightsService.enableCollection(metricType, isCurrentMetricEnabled);
        updateQueryInsightsState();
    }

    /**
     * Set search query metrics enabled to enable collection of search query categorization metrics.
     * @param searchQueryMetricsEnabled boolean flag
     */
    public void setSearchQueryMetricsEnabled(boolean searchQueryMetricsEnabled) {
        this.queryInsightsService.enableSearchQueryMetricsFeature(searchQueryMetricsEnabled);
        updateQueryInsightsState();
    }

    public void setGroupingFieldNameEnabled(Boolean fieldNameEnabled) {
        this.groupingFieldNameEnabled = fieldNameEnabled;
    }

    public void setGroupingFieldTypeEnabled(Boolean fieldTypeEnabled) {
        this.groupingFieldTypeEnabled = fieldTypeEnabled;
    }

    /**
     * Update the query insights service state based on the enabled features.
     * If any feature is enabled, it starts the service. If no features are enabled, it stops the service.
     */
    private void updateQueryInsightsState() {
        boolean anyFeatureEnabled = queryInsightsService.isAnyFeatureEnabled();

        if (anyFeatureEnabled && !super.isEnabled()) {
            super.setEnabled(true);
            queryInsightsService.stop(); // Ensures a clean restart
            queryInsightsService.start();
        } else if (!anyFeatureEnabled && super.isEnabled()) {
            super.setEnabled(false);
            queryInsightsService.stop();
        }
    }

    @Override
    public boolean isEnabled() {
        return super.isEnabled();
    }

    @Override
    public void onPhaseStart(SearchPhaseContext context) {}

    @Override
    public void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

    @Override
    public void onPhaseFailure(SearchPhaseContext context, Throwable cause) {}

    @Override
    public void onRequestStart(SearchRequestContext searchRequestContext) {}

    @Override
    public void onRequestEnd(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        constructSearchQueryRecord(context, searchRequestContext);
    }

    @Override
    public void onRequestFailure(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        constructSearchQueryRecord(context, searchRequestContext);
    }

    private boolean skipSearchRequest(final SearchRequestContext searchRequestContext) {
        // Skip profile queries
        if (Optional.ofNullable(searchRequestContext)
            .map(SearchRequestContext::getRequest)
            .map(SearchRequest::source)
            .map(SearchSourceBuilder::profile)
            .orElse(false)) {
            log.info("Skipping profile query");
            return true;
        }

        if (excludedIndicesPattern.isEmpty()) {
            return false;
        }

        boolean shouldSkip = Optional.ofNullable(searchRequestContext)
            .map(SearchRequestContext::getSuccessfulSearchShardIndices)
            .map(indices -> {
                String indexNames = indices.stream().map(Index::getName).collect(Collectors.joining(","));
                log.info("Checking indices: {}", indexNames);
                return indices.stream().map(Index::getName).anyMatch(this::matchedExcludedIndices);
            })
            .orElse(false);

        if (shouldSkip) {
            log.info("Skipping excluded indices");
        }
        return shouldSkip;
    }

    private boolean matchedExcludedIndices(String indexName) {
        if (indexName == null || excludedIndicesPattern == null) {
            return false;
        }
        return excludedIndicesPattern.stream().anyMatch(pattern -> pattern.matcher(indexName).matches());
    }

    private void constructSearchQueryRecord(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        String indices = Optional.ofNullable(context.getRequest().indices())
            .map(arr -> String.join(",", arr))
            .orElse("unknown");
        log.info("Processing search request for indices: {}", indices);

        if (skipSearchRequest(searchRequestContext)) {
            return;
        }

        SearchTask searchTask = context.getTask();
        List<TaskResourceInfo> tasksResourceUsages = searchRequestContext.getPhaseResourceUsage();
        tasksResourceUsages.add(
            new TaskResourceInfo(
                searchTask.getAction(),
                searchTask.getId(),
                searchTask.getParentTaskId().getId(),
                clusterService.localNode().getId(),
                searchTask.getTotalResourceStats()
            )
        );

        final SearchRequest request = context.getRequest();
        try {
            Map<MetricType, Measurement> measurements = new HashMap<>();
            measurements.put(
                MetricType.LATENCY,
                new Measurement(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchRequestContext.getAbsoluteStartNanos()))
            );
            measurements.put(
                MetricType.CPU,
                new Measurement(
                    tasksResourceUsages.stream().map(a -> a.getTaskResourceUsage().getCpuTimeInNanos()).mapToLong(Long::longValue).sum()
                )
            );
            measurements.put(
                MetricType.MEMORY,
                new Measurement(
                    tasksResourceUsages.stream().map(a -> a.getTaskResourceUsage().getMemoryInBytes()).mapToLong(Long::longValue).sum()
                )
            );

            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.SEARCH_TYPE, request.searchType().toString().toLowerCase(Locale.ROOT));
            attributes.put(Attribute.SOURCE, request.source());
            attributes.put(Attribute.TOTAL_SHARDS, context.getNumShards());
            attributes.put(Attribute.INDICES, request.indices());
            attributes.put(Attribute.PHASE_LATENCY_MAP, searchRequestContext.phaseTookMap());
            attributes.put(Attribute.TASK_RESOURCE_USAGES, tasksResourceUsages);
            attributes.put(Attribute.GROUP_BY, QueryInsightsSettings.DEFAULT_GROUPING_TYPE);
            attributes.put(Attribute.NODE_ID, clusterService.localNode().getId());
            attributes.put(Attribute.TOP_N_QUERY, new HashMap<>(DEFAULT_TOP_N_QUERY_MAP));

            if (queryInsightsService.isGroupingEnabled() || log.isTraceEnabled()) {
                // Generate the query shape only if grouping is enabled or trace logging is enabled
                final String queryShape = queryShapeGenerator.buildShape(
                    request.source(),
                    groupingFieldNameEnabled,
                    groupingFieldTypeEnabled,
                    searchRequestContext.getSuccessfulSearchShardIndices()
                );

                // Print the query shape if tracer is enabled
                if (log.isTraceEnabled()) {
                    log.trace("Query Shape:\n{}", queryShape);
                }

                // Add hashcode attribute when grouping is enabled
                if (queryInsightsService.isGroupingEnabled()) {
                    String hashcode = queryShapeGenerator.getShapeHashCodeAsString(queryShape);
                    attributes.put(Attribute.QUERY_GROUP_HASHCODE, hashcode);
                }
            }

            Map<String, Object> labels = new HashMap<>();
            // Retrieve user provided label if exists
            String userProvidedLabel = context.getTask().getHeader(Task.X_OPAQUE_ID);
            if (userProvidedLabel != null) {
                labels.put(Task.X_OPAQUE_ID, userProvidedLabel);
            }
            attributes.put(Attribute.LABELS, labels);

            // Construct SearchQueryRecord from attributes and measurements
            SearchQueryRecord record = new SearchQueryRecord(request.getOrCreateAbsoluteStartMillis(), measurements, attributes);
            queryInsightsService.addRecord(record);

            // Added
            // Export query data
            exportQueryData(record);

        } catch (Exception e) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.DATA_INGEST_EXCEPTIONS);
            log.error(String.format(Locale.ROOT, "fail to ingest query insight data, error: %s", e));
        }
    }

    /**
     * Validate the index name for excluded indices
     * @param excludedIndices list of index to validate
     */
    public void validateExcludedIndices(@NonNull List<String> excludedIndices) {
        for (String index : excludedIndices) {
            if (index == null) {
                throw new IllegalArgumentException("Excluded index name cannot be null.");
            }
            if (index.isBlank()) {
                throw new IllegalArgumentException("Excluded index name cannot be blank.");
            }
            if (index.chars().anyMatch(Character::isUpperCase)) {
                throw new IllegalArgumentException("Index name must be lowercase.");
            }
        }
    }


    // Added:
    private synchronized void exportQueryData(SearchQueryRecord record) {
        queryBuffer.add(record);
        if (queryBuffer.size() >= BATCH_SIZE) {
            List<SearchQueryRecord> batch = new ArrayList<>(queryBuffer);
            queryBuffer.clear();
            clusterService.getClusterApplierService().threadPool().executor(QUERY_INSIGHTS_EXECUTOR).execute(() -> writeQueryBatch(batch));
        }
    }

    // Return shard count for indices in the query
    private int getActiveShardCount(SearchQueryRecord record) {
        ClusterState clusterState = clusterService.state();
        int totalShards = 0;
        String[] indices = (String[]) record.getAttributes().get(Attribute.INDICES);

        for (String indexName : indices) {
            try {
                totalShards += clusterState.routingTable()
                    .index(indexName)
                    .primaryShardsActive();
            } catch (Exception e) {
                // Index might not exist, skip
            }
        }
        return totalShards;
    }



    // Added
    private void writeQueryBatch(List<SearchQueryRecord> records) {
        try (java.io.FileWriter writer = new java.io.FileWriter(queryExportFilePath, true)) {
            for (SearchQueryRecord record : records) {
                Object source = record.getAttributes().get(Attribute.SOURCE);
                String queryJson = source != null ? source.toString() : "{}";
                String queryType = getQueryType(record);

                String formattedDate = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(record.getTimestamp()));
                String[] indices = (String[]) record.getAttributes().get(Attribute.INDICES);
                String indicesStr = (indices != null && indices.length > 0) ? String.join(";", indices) : "_all";
                
                writer.append(String.format(
                    "{\"timestamp\":\"%s\",\"query_type\":\"%s\",\"latency_ms\":%d,\"cpu_nanos\":%d,\"memory_bytes\":%d,\"document_count\":%d,\"search_type\":\"%s\",\"indices\":\"%s\",\"total_shards\":%d,\"active_shard_count\":%d,\"node_id\":\"%s\",\"requested_size\":%d,\"query\":%s}\n",
                    formattedDate,
                    queryType,
                    record.getMeasurement(MetricType.LATENCY).longValue(),
                    record.getMeasurement(MetricType.CPU).longValue(),
                    record.getMeasurement(MetricType.MEMORY).longValue(),
                    getDocumentCount(record),
                    record.getAttributes().get(Attribute.SEARCH_TYPE),
                    indicesStr,
                    (Integer) record.getAttributes().get(Attribute.TOTAL_SHARDS),
                    getActiveShardCount(record),
                    record.getAttributes().get(Attribute.NODE_ID),
                    getRequestedSize(record),
                    queryJson
                ));
            }
        } catch (IOException e) {
            log.error("Failed to write to CSV file: {}", e.getMessage());
        }
    }


    // Attempt at returning document size
    private int getRequestedSize(SearchQueryRecord record) {
        Object source = record.getAttributes().get(Attribute.SOURCE);
        if (source != null) {
            String sourceStr = source.toString();
            if (sourceStr.contains("\"size\":")) {
                try {
                    int sizeIndex = sourceStr.indexOf("\"size\":");
                    String sizeSubstring = sourceStr.substring(sizeIndex + 7);
                    int commaIndex = sizeSubstring.indexOf(",");
                    int braceIndex = sizeSubstring.indexOf("}");
                    int endIndex = (commaIndex != -1 && commaIndex < braceIndex) ? commaIndex : braceIndex;
                    return Integer.parseInt(sizeSubstring.substring(0, endIndex).trim());
                } catch (Exception e) {
                    return -1;
                }
            }
        }
        return 0; // Default size
    }

    // Return exact document count for indices in the query
    private long getDocumentCount(SearchQueryRecord record) {
        if (client == null) {
            return 0;
        }
        
        String[] indices = (String[]) record.getAttributes().get(Attribute.INDICES);
        String cacheKey = (indices != null && indices.length > 0) ? String.join(",", indices) : "_all";
        
        // Check cache first
        if (docCountCache.containsKey(cacheKey)) {
            return docCountCache.get(cacheKey);
        }
        
        // Not in cache, make API call
        try {
            IndicesStatsRequest request = new IndicesStatsRequest();
            request.indices(indices);
            request.docs(true);

            IndicesStatsResponse response = client.admin().indices().stats(request).actionGet();
            long count = response.getTotal().getDocs() != null ? response.getTotal().getDocs().getCount() : 0;
            
            // Cache the result
            docCountCache.put(cacheKey, count);
            return count;
        } catch (Exception e) {
            // Cache 0 to avoid repeated failures
            docCountCache.put(cacheKey, 0L);
            return 0;
        }
    }

    private String getQueryType(SearchQueryRecord record) {
        Object source = record.getAttributes().get(Attribute.SOURCE);
        if (source != null) {
            String sourceStr = source.toString().toLowerCase();
            if (sourceStr.contains("range")) return "RANGE";
            if (sourceStr.contains("match_all")) return "MATCH_ALL";
            if (sourceStr.contains("match_phrase")) return "MATCH_PHRASE";
            if (sourceStr.contains("match")) return "MATCH";
            if (sourceStr.contains("multi_match")) return "MULTI_MATCH";
            if (sourceStr.contains("term")) return "TERM";
            if (sourceStr.contains("terms")) return "TERMS";
            if (sourceStr.contains("bool")) return "BOOL";
            if (sourceStr.contains("wildcard")) return "WILDCARD";
            if (sourceStr.contains("prefix")) return "PREFIX";
            if (sourceStr.contains("fuzzy")) return "FUZZY";
            if (sourceStr.contains("regexp")) return "REGEXP";
            if (sourceStr.contains("exists")) return "EXISTS";
            if (sourceStr.contains("ids")) return "IDS";
        }
        return "UNKNOWN";
    }

}
