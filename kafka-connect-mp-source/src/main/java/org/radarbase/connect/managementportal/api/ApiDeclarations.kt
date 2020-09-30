package org.radarbase.connect.managementportal.api

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant
import java.time.LocalDateTime
import java.util.HashMap
import java.time.ZonedDateTime


data class RecordDataDTO(
        var projectId: String?,
        var userId: String?,
        var sourceId: String?,
        var time: LocalDateTime? = null,
        var timeZoneOffset: Int? = null,
        var contents: Set<ContentsDTO>? = null)

data class RevisionDTO(
        var id: Long?,
        var timestamp: Instant?,
        var author: String?,
        var changes: Object?)

data class ProjectDTO (
        var id: Long?,
        var projectName: String?
)

data class SubjectDTO (
    var id: Long?,
    var login: String?,
    var externalLink: String?,
    var externalId: String?,
    var status: String?,
    var createdBy: String?,
    var createdDate: ZonedDateTime?,
    var lastModifiedBy: String?,
    var lastModifiedDate: String?,
    var project: ProjectDTO,
    var attributes: HashMap<String, String>
)

data class OauthToken(
        @JsonProperty("access_token") var accessToken: String,
        @JsonProperty("expires_in") var expiresIn: Long,
        @JsonProperty("iat") var issuedAt: Long,
        @JsonProperty("scope") var scope: String,
        @JsonProperty("sub") var sub: String)
