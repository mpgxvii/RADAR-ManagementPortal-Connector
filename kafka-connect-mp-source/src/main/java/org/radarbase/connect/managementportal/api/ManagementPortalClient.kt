package org.radarbase.connect.managementportal.api

import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import okio.BufferedSink
import org.radarbase.connect.managementportal.converter.Log
import org.radarbase.connect.managementportal.exception.BadGatewayException
import org.radarbase.connect.managementportal.exception.ConflictException
import org.radarbase.connect.managementportal.exception.NotAuthorizedException
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.time.Instant
import javax.xml.datatype.DatatypeConstants.SECONDS
import java.time.ZonedDateTime
import java.time.ZoneOffset.UTC
import java.util.*
import kotlin.collections.LinkedHashMap


open class ManagementPortalClient(
        auth: Authenticator,
        private var httpClient: OkHttpClient,
        private var managementPortalApiUrl: String) : Closeable {

    init {
        httpClient = httpClient
                .newBuilder()
                .authenticator(auth)
                .build()

        managementPortalApiUrl = managementPortalApiUrl.trimEnd('/')
    }

    fun pollRecords(startDate: Instant): List<LinkedHashMap<String, Any>> = httpClient.executeRequest {
        logger.info("Polling, offset is: ${startDate}")
        var endDate= Instant.now()

        url("$managementPortalApiUrl/revisions?fromDate=${startDate}&toDate=${endDate}").get();
    }

    fun retrieveSubject(subjectId: String): SubjectDTO = httpClient.executeRequest {
        url("$managementPortalApiUrl/subjects/$subjectId").get();
    }

    override fun close() {
    }

    private inline fun <reified T: Any> OkHttpClient.executeRequest(
            noinline requestBuilder: Request.Builder.() -> Request.Builder): T = executeRequest(requestBuilder) { response ->
        mapper.readValue(response.body?.byteStream(), T::class.java)
                ?: throw IOException("Received invalid response")
    }

    private fun <T> OkHttpClient.executeRequest(requestBuilder: Request.Builder.() -> Request.Builder, handling: (Response) -> T): T {
        val request = Request.Builder().requestBuilder().build()
        return this.newCall(request).execute().use { response ->
            if (response.isSuccessful) {
                logger.info("Request to ${request.url} is SUCCESSFUL")
                return handling(response)
            } else {
                logger.info("Request to ${request.url} has FAILED with response-code ${response.code}")
                when (response.code) {
                    401 -> throw NotAuthorizedException("access token is not provided or is invalid : ${response.message}")
                    403 -> throw NotAuthorizedException("access token is not authorized to perform this request")
                    409 -> throw ConflictException("Conflicting request exception: ${response.message}")
                }
                throw BadGatewayException("Failed to make request to ${request.url}: Error code ${response.code}:  ${response.body?.string()}")
            }
        }
    }

    private fun Any.toJsonBody(mediaType: MediaType = APPLICATION_JSON): RequestBody = mapper
            .writeValueAsString(this)
            .toRequestBody(mediaType)

    companion object {
        private val logger = LoggerFactory.getLogger(ManagementPortalClient::class.java)
        private val APPLICATION_JSON = "application/json; charset=utf-8".toMediaType()
        private val TEXT_PLAIN = "text/plain; charset=utf-8".toMediaType()
        private var mapper: ObjectMapper = ObjectMapper()
                .registerModule(KotlinModule())
                .registerModule(JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    }
}
