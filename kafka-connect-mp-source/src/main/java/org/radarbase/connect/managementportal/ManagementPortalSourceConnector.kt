package org.radarbase.connect.managementportal

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.radarbase.connect.managementportal.util.VersionUtil
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.collections.HashMap

class ManagementPortalSourceConnector : SourceConnector() {
    private lateinit var connectorConfig: ManagementPortalSourceConnectorConfig

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> =
            Collections.nCopies(maxTasks, HashMap(connectorConfig.originalsStrings()))

    override fun start(props: Map<String, String>?) {
        connectorConfig = ManagementPortalSourceConnectorConfig(props!!)
    }

    override fun stop() {
        logger.debug("Stopping source task")
    }

    override fun version(): String = VersionUtil.getVersion()

    override fun taskClass(): Class<out Task> = ManagementPortalSourceTask::class.java

    override fun config(): ConfigDef = ManagementPortalSourceConnectorConfig.conf()

    companion object {
        private val logger = LoggerFactory.getLogger(ManagementPortalSourceConnector::class.java)
    }

}
