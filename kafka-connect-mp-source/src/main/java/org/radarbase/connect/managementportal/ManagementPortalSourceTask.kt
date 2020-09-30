package org.radarbase.connect.managementportal

import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.radarbase.connect.managementportal.ManagementPortalSourceConnectorConfig.Companion.SOURCE_POLL_INTERVAL_CONFIG
import org.radarbase.connect.managementportal.api.*
import org.radarbase.connect.managementportal.converter.ConverterLogRepository
import org.radarbase.connect.managementportal.converter.LogRepository
import org.radarbase.connect.managementportal.exception.ConversionFailedException
import org.radarbase.connect.managementportal.util.VersionUtil
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.temporal.Temporal
import java.time.temporal.TemporalAmount
import java.time.temporal.ChronoUnit.NANOS

import java.util.*
import kotlin.collections.LinkedHashMap
import org.radarbase.connect.managementportal.converter.RevisionConverter


class ManagementPortalSourceTask : SourceTask() {
    private var pollInterval = Duration.ofMinutes(1)
    private var failedPollInterval = Duration.ofSeconds(6)
    private lateinit var client: ManagementPortalClient
    private lateinit var logRepository: LogRepository
    private lateinit var nextPoll: Instant
    private lateinit var revisionConverter: RevisionConverter

    val TIMESTAMP_OFFSET_KEY = "timestamp"
    val DEFAULT_PARTITION_MAP = Collections.singletonMap("topic", "subjects");

    override fun start(props: Map<String, String>?) {
        var offset: Map<String, Any>? = null

        val connectConfig = ManagementPortalSourceConnectorConfig(props!!)
        val httpClient = connectConfig.httpClient
        nextPoll = Instant.EPOCH
        client = ManagementPortalClient(
                connectConfig.getAuthenticator(),
                httpClient,
                connectConfig.managementPortalApiUrl)

        logRepository = ConverterLogRepository()
        revisionConverter = RevisionConverter(client, logRepository)
        pollInterval = Duration.ofMillis(connectConfig.getLong(SOURCE_POLL_INTERVAL_CONFIG))
        failedPollInterval = pollInterval.dividedBy(10)

        logger.info("Poll with interval $pollInterval milliseconds")

        offset = context.offsetStorageReader().offset(getPartition())
        logger.info("The partition offsets are {}", offset);
    }

    override fun stop() {
        logger.debug("Stopping source task")
        client.close()
    }

    override fun version(): String = VersionUtil.getVersion()

    fun getPartition(): Map<String, String> = DEFAULT_PARTITION_MAP

    override fun poll(): List<SourceRecord> {
        var offsets = context.offsetStorageReader().offset(getPartition())
        var date: Instant = Instant.ofEpochMilli(1L)

        if(offsets != null)  date =  (Instant.ofEpochMilli(offsets.getOrDefault(TIMESTAMP_OFFSET_KEY, 1L) as Long)).plusMillis(1)

        logger.info("Polling new records...")
        val timeout = nextPoll.untilNow().toMillis()
        if (timeout > 0) {
            logger.info("Waiting {} milliseconds for next polling time", timeout)
            Thread.sleep(timeout)
        }

        val records: List<LinkedHashMap<String, Any>> = try {
            client.pollRecords(date)
        } catch (exe: Exception) {
            logger.info("Could not successfully poll records. Waiting for next polling...")
            logger.info(exe.toString())
            nextPoll = failedPollInterval.fromNow()
            return emptyList()
        }

        nextPoll = pollInterval.fromNow()

        logger.info("Received ${records.size} records at $nextPoll")

        return records.map { record -> processRecord(record) }.filterNotNull()
    }

    private fun processRecord(record: LinkedHashMap<String, Any>): SourceRecord? {
        return try {
            revisionConverter.convert(record)
        } catch (exe: ConversionFailedException) {
            logger.error("Could not convert record ${record}", exe)
            null
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ManagementPortalSourceTask::class.java)

        private fun Temporal.untilNow(): Duration = Duration.between(Instant.now(), this)
        private fun TemporalAmount.fromNow(): Instant = Instant.now().plus(this)
    }
}

