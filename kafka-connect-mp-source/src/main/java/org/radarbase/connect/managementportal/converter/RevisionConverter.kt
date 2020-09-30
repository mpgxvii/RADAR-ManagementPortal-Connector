package org.radarbase.connect.managementportal.converter

import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.source.SourceRecord
import org.radarbase.connect.managementportal.exception.ConversionFailedException
import org.radarbase.connect.managementportal.exception.ConversionTemporarilyFailedException
import org.radarcns.kafka.ObservationKey
import org.slf4j.LoggerFactory
import java.io.IOException
import io.confluent.connect.avro.AvroDataConfig
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.radarbase.connect.managementportal.api.*
import java.text.SimpleDateFormat
import java.util.*
import kotlin.collections.HashMap



class RevisionConverter(
        private val client: ManagementPortalClient,
        private val logRepository: LogRepository,
        private val avroData: AvroData = AvroData(AvroDataConfig.Builder()
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 20)
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .build())
)  {

    val DATE_FORMAT = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val TIMESTAMP_OFFSET_KEY = "timestamp"
    val REVISION_CHANGES_KEY = "changes"
    val REVISION_CHANGES_ADD = "ADD"
    val SUBJECT_KEY = "subject"
    val SUBJECT_USER_KEY = "user"
    val SUBJECT_USER_LOGIN_KEY = "login"
    val MANAGEMENT_PORTAL_SUBJECT_TOPIC = "connect_rest_management_portal_subjects"

    val SUBJECT_ID_FIELD = "subjectId"
    val ID_FIELD = "id"
    val RECORD_ID_FIELD = "recordId"
    val STATUS_FIELD = "status"
    val CREATED_DATE_FIELD = "createdDate"
    val PROJECT_ID_FIELD = "projectId"
    val HUMAN_READABLE_ID_FIELD = "humanReadableId"
    val SITE_FIELD = "site"

    val DEFAULT_PARTITION_MAP = Collections.singletonMap("topic", "subjects");

    fun convert(record: LinkedHashMap<String, Any>): SourceRecord? {
        val recordId = (checkNotNull(record.get("id")) as Integer).toLong()
        val recordLogger = logRepository.createLogger(logger, recordId)
        recordLogger.info("Converting record: record-id $recordId")

        try {
            var recordTimestamp = DATE_FORMAT.parse(record.get(TIMESTAMP_OFFSET_KEY) as String).toInstant().toEpochMilli()
            val offsets = Collections.singletonMap(TIMESTAMP_OFFSET_KEY, recordTimestamp)

            return try {
                var entityAddition = ((record.get(REVISION_CHANGES_KEY)) as HashMap<String, Any>).get(REVISION_CHANGES_ADD) as HashMap<String, Any>
                var subjectList = entityAddition.getOrDefault(SUBJECT_KEY, Collections.emptyList<Object>()) as List<Object>;

                    return try {
                        var subjectMetadata = subjectList.get(0) as HashMap<String, HashMap<String, String>>;
                        var subjectId = subjectMetadata.get(SUBJECT_USER_KEY)?.get(SUBJECT_USER_LOGIN_KEY) ?: "";
                        val subject = client.retrieveSubject(subjectId)
                        val keyData = RecordDataDTO(subject.project.projectName, subject.login, "")
                        var key = keyData.computeObservationKey(avroData)
                        val value = convertSubject(subject, recordLogger)

                        SourceRecord(getPartition(), offsets, MANAGEMENT_PORTAL_SUBJECT_TOPIC, key?.schema(), key?.value(), value?.schema(), value)
                    } catch (exe: Exception) {
                        recordLogger.info("This revision does not contain subject data")
                        null
                    }
            }
            catch(exe: Exception) {
                recordLogger.info("This revision does not contain entity data")
                null
            }
        } catch (exe: IOException) {
            recordLogger.error("Could not convert record $recordId", exe)
            throw ConversionTemporarilyFailedException("Could not convert record $recordId", exe)
        }
    }

    fun convertSubject(subject: SubjectDTO, recordLogger: RecordLogger): Struct {
        try {
            val convertedSubject = Struct(getSubjectSchema())

            convertedSubject.put(ID_FIELD, subject.id)
            convertedSubject.put(SUBJECT_ID_FIELD, subject.login)
            convertedSubject.put(RECORD_ID_FIELD, subject.externalId)
            convertedSubject.put(STATUS_FIELD, subject.status)
            convertedSubject.put(CREATED_DATE_FIELD, subject.createdDate.toString())
            convertedSubject.put(PROJECT_ID_FIELD, subject.project.projectName)
            convertedSubject.put(HUMAN_READABLE_ID_FIELD, subject.attributes.getOrDefault("Human-readable-identifier", "none"))
            convertedSubject.put(SITE_FIELD, subject.attributes.getOrDefault("site", "none"))

            recordLogger.info("Subject converted is ${convertedSubject}")

            return convertedSubject
        } catch (exe: Exception) {
            recordLogger.error("Could not convert subject", exe)
            throw ConversionFailedException("Could not convert subject",exe)
        }
    }

    fun getSubjectSchema(): Schema {
        val schemaBuilder = SchemaBuilder.struct().name("subjectSchema")
        schemaBuilder.field(ID_FIELD, Schema.INT64_SCHEMA);
        schemaBuilder.field(SUBJECT_ID_FIELD, Schema.STRING_SCHEMA);
        schemaBuilder.field(RECORD_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        schemaBuilder.field(STATUS_FIELD, Schema.STRING_SCHEMA);
        schemaBuilder.field(CREATED_DATE_FIELD, Schema.STRING_SCHEMA);
        schemaBuilder.field(PROJECT_ID_FIELD, Schema.STRING_SCHEMA);
        schemaBuilder.field(HUMAN_READABLE_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        schemaBuilder.field(SITE_FIELD, Schema.OPTIONAL_STRING_SCHEMA);

        val schema = schemaBuilder.build()
        return schema
    }

    fun getPartition(): Map<String, String> = DEFAULT_PARTITION_MAP

    fun close() {
        this.client.close()
    }

    private fun RecordDataDTO.computeObservationKey(avroData: AvroData): SchemaAndValue {
        return avroData.toConnectData(
                ObservationKey.getClassSchema(),
                ObservationKey(
                        projectId,
                        userId,
                        sourceId
                )
        )
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RevisionConverter::class.java)
    }
}
