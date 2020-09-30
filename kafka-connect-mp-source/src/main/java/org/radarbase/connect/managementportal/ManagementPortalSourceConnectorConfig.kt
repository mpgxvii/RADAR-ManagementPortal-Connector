package org.radarbase.connect.managementportal

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import okhttp3.Authenticator
import okhttp3.OkHttpClient
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.radarbase.connect.managementportal.auth.ClientCredentialsAuthorizer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class ManagementPortalSourceConnectorConfig(config: ConfigDef, parsedConfig: Map<String, String>) :
        AbstractConfig(config, parsedConfig) {

    private lateinit var authenticator: Authenticator

    fun getAuthenticator(): Authenticator {
        if(!::authenticator.isInitialized) {
            logger.info("Initializing authenticator")
            authenticator = ClientCredentialsAuthorizer(
                    httpClient,
                    oauthClientId,
                    oauthClientSecret,
                    tokenRequestUrl)
        }
        return authenticator
    }

    constructor(parsedConfig: Map<String, String>) : this(conf(), parsedConfig)

    val oauthClientId: String = getString(CONNECT_MP_CLIENT_CONFIG)

    val oauthClientSecret: String = getString(CONNECT_MP_SECRET_CONFIG)

    val tokenRequestUrl: String = getString(CONNECT_MP_CLIENT_TOKEN_URL_CONFIG)

    val managementPortalApiUrl: String = getString(CONNECT_MP_API_URL_CONFIG)

    val httpClient: OkHttpClient
        get() = globalHttpClient

    companion object {

        val logger: Logger = LoggerFactory.getLogger(ManagementPortalSourceConnectorConfig::class.java)

        const val CONNECT_MP_CLIENT_CONFIG = "connect.source.client.id"
        private const val CONNECT_MP_CLIENT_DOC = "Client ID for the for the connector"
        private const val CONNECT_MP_CLIENT_DISPLAY = "Connector client ID"
        private const val CONNECT_MP_CLIENT_DEFAULT = "connector-client"

        const val CONNECT_MP_SECRET_CONFIG = "connect.source.client.secret"
        private const val CONNECT_MP_SECRET_DOC = "Secret for the connector client set in connect.source.mp.client."
        private const val CONNECT_MP_SECRET_DISPLAY = "Connector client secret"

        const val CONNECT_MP_CLIENT_TOKEN_URL_CONFIG = "connect.source.client.tokenUrl"
        private const val CONNECT_MP_CLIENT_TOKEN_URL_DOC = "Complete Token URL to get access token"
        private const val CONNECT_MP_CLIENT_TOKEN_URL_DISPLAY = "Token URL to get access token"
        private const val CONNECT_MP_CLIENT_TOKEN_URL_DEFAULT = "http://managementportal-app:8080/managementportal/oauth/token"

        const val CONNECT_MP_API_URL_CONFIG = "connect.source.mp.apiUrl"
        private const val CONNECT_MP_API_URL_DOC = "URL of the Management Portal API"
        private const val CONNECT_MP_API_URL_DISPLAY = "URL of MP API"
        private const val CONNECT_MP_API_URL_DEFAULT = "http://managementportal-app:8080/managementportal/api"

        const val SOURCE_POLL_INTERVAL_CONFIG = "connect.source.poll.interval.ms"
        private const val SOURCE_POLL_INTERVAL_DOC = "How often to poll the source URL."
        private const val SOURCE_POLL_INTERVAL_DISPLAY = "Polling interval"
        private const val SOURCE_POLL_INTERVAL_DEFAULT = 60000L

        var mapper: ObjectMapper = ObjectMapper()
                .registerModule(KotlinModule())
                .registerModule(JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

        fun conf(): ConfigDef {
            val groupName = "mp"
            var orderInGroup = 0

            return ConfigDef()
                    .define(SOURCE_POLL_INTERVAL_CONFIG,
                            ConfigDef.Type.LONG,
                            SOURCE_POLL_INTERVAL_DEFAULT,
                            ConfigDef.Importance.HIGH,
                            SOURCE_POLL_INTERVAL_DOC,
                            groupName,
                            ++orderInGroup,
                            ConfigDef.Width.SHORT,
                            SOURCE_POLL_INTERVAL_DISPLAY)

                    .define(CONNECT_MP_CLIENT_CONFIG,
                            ConfigDef.Type.STRING,
                            CONNECT_MP_CLIENT_DEFAULT,
                            ConfigDef.Importance.HIGH,
                            CONNECT_MP_CLIENT_DOC,
                            groupName,
                            ++orderInGroup,
                            ConfigDef.Width.SHORT,
                            CONNECT_MP_CLIENT_DISPLAY)

                    .define(CONNECT_MP_SECRET_CONFIG,
                            ConfigDef.Type.STRING,
                            "",
                            ConfigDef.Importance.HIGH,
                            CONNECT_MP_SECRET_DOC,
                            groupName,
                            ++orderInGroup,
                            ConfigDef.Width.SHORT,
                            CONNECT_MP_SECRET_DISPLAY)

                    .define(CONNECT_MP_API_URL_CONFIG,
                            ConfigDef.Type.STRING,
                            CONNECT_MP_API_URL_DEFAULT,
                            ConfigDef.Importance.LOW,
                            CONNECT_MP_API_URL_DOC,
                            groupName,
                            ++orderInGroup,
                            ConfigDef.Width.SHORT,
                            CONNECT_MP_API_URL_DISPLAY)

                    .define(CONNECT_MP_CLIENT_TOKEN_URL_CONFIG,
                            ConfigDef.Type.STRING,
                            CONNECT_MP_CLIENT_TOKEN_URL_DEFAULT,
                            ConfigDef.Importance.LOW,
                            CONNECT_MP_CLIENT_TOKEN_URL_DOC,
                            groupName,
                            ++orderInGroup,
                            ConfigDef.Width.SHORT,
                            CONNECT_MP_CLIENT_TOKEN_URL_DISPLAY)

        }

        private val globalHttpClient = OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .writeTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build()
    }
}
