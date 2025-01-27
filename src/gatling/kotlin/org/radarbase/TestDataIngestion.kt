package org.radarbase

import com.fasterxml.jackson.core.JsonFactory
import com.typesafe.config.ConfigFactory
import io.gatling.javaapi.core.CoreDsl.*
import io.gatling.javaapi.core.Session
import io.gatling.javaapi.core.Simulation
import io.gatling.javaapi.http.HttpDsl.http
import io.gatling.javaapi.http.HttpDsl.status
import org.apache.avro.Schema
import org.radarbase.data.RemoteSchemaEncoder
import org.radarbase.producer.schema.ParsedSchemaMetadata
import org.radarcns.active.questionnaire.Answer
import org.radarcns.active.questionnaire.Questionnaire
import org.radarcns.kafka.ObservationKey
import org.radarcns.kafka.RecordSet
import java.io.StringWriter
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.random.Random

class TestDataIngestion : Simulation() {

    private val conf = ConfigFactory.parseResources("test.conf")
    private val baseURL = conf.getString("baseUrl")
    private val clientId = conf.getString("clientId")
    private val clientSecret = conf.getString("clientSecret")
    private val mpAdminUsername = conf.getString("mpAdminUsername")
    private val mpAdminPassword = conf.getString("mpAdminPassword")
    private val clientIdToPair = conf.getString("clientIdToPair")
    private val kafkaApiVersion = conf.getInt("kafkaApiVersion")
    private val projectName = conf.getString("projectName")
    private val organizationName = conf.getString("organizationName")
    private val clientIdToPairSecret = conf.getString("clientIdToPairSecret")
    private val numberOfParticipants = conf.getInt("numberOfParticipants")
    private val sourceTypeProducer = conf.getString("sourceTypeProducer")
    private val sourceTypeModel = conf.getString("sourceTypeModel")
    private val sourceTypeCatalogVersion = conf.getString("sourceTypeCatalogVersion")
    private val keySchemaIds = ConcurrentHashMap<String, Int>()
    private val valueSchemaIds = ConcurrentHashMap<String, Int>()
    private val loginToAccessToken = ConcurrentHashMap<String, String>()
    private val externalIdToLogin = ConcurrentHashMap<String, String>()
    private val random = Random
    private var projectDto: String? = null
    private val topics = csv("topics.csv").readRecords()
    private val sourceTypeDtos = ConcurrentHashMap<String, String>()
    private val projectSourceIds = ConcurrentHashMap<String, String>()
    private val projectSourceDtos = ConcurrentHashMap<String, String>()
    private val messageNumber = conf.getString("messageNumber").toInt()
    private val messageSize = conf.getString("messageSize").toInt()

    val userDetailsFeeder = csv("users.csv")

    val loginFeeder = object : Iterator<Map<String, String>> {
        override fun hasNext() = true
        override fun next(): Map<String, String> {
            val externalId = externalIdToLogin.keys.toList()[random.nextInt(externalIdToLogin.size)]
            return mapOf(
                "externalId" to externalId,
                "login" to externalIdToLogin[externalId]!!
            )
        }
    }

    val httpConf = http
        .baseUrl(baseURL)
        .inferHtmlResources()
        .acceptHeader("*/*")
        .acceptEncodingHeader("gzip, deflate")
        .acceptLanguageHeader("fr,fr-fr;q=0.8,en-us;q=0.5,en;q=0.3")
        .connectionHeader("keep-alive")
        .userAgentHeader("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:33.0) Gecko/20100101 Firefox/33.0")

    val authorizationHeader = "Basic " + Base64.getEncoder().encodeToString("$clientId:$clientSecret".toByteArray(
        StandardCharsets.UTF_8))

    val authorizationHeaderForClient = "Basic " + Base64.getEncoder().encodeToString("$clientIdToPair:$clientIdToPairSecret".toByteArray(StandardCharsets.UTF_8))

    val headers_http_authentication = mapOf(
        "Content-Type" to "application/x-www-form-urlencoded",
        "Accept" to "application/json",
        "Authorization" to authorizationHeader
    )

    val headers_http_authenticated_for_client = mapOf(
        "Content-Type" to "application/x-www-form-urlencoded",
        "Accept" to "application/json",
        "Authorization" to authorizationHeaderForClient
    )

    val headers_http_authenticated_for_mp_admin = mapOf(
        "Accept" to "application/json",
        "Content-Type" to "application/json",
        "Authorization" to "Bearer #{mp_access_token}"
    )

    val headers_http_authenticated_for_subject = mapOf(
        "Content-Type" to "application/json",
        "Accept" to "application/json",
        "Authorization" to "Bearer #{access_token_for_subject}"
    )

    val ensureOrganization =
        exec(
            http("Get organization details by organization name: $organizationName")
                .get("/managementportal/api/organizations/$organizationName")
                .headers(headers_http_authenticated_for_mp_admin)
                .check(
                    status().within(200, 404),
                    jsonPath("$.name").withDefault("").saveAs("organizationName"),
                    jsonPath("$").withDefault("").saveAs("organizationDTO")
                )
        )
        .doIf({ session -> session.getString("organizationName")?.isEmpty() }).then(
            exec(http("Create organization")
                .post("/managementportal/api/organizations")
                .headers(headers_http_authenticated_for_mp_admin)
                .body(StringBody(::organizationBody)).asJson()
                .check(status().shouldBe(201))
                .check(jsonPath("$").ofString().saveAs("organizationDTO"))
            )
        ).exitHereIfFailed()

    val ensureProject =
        exec(
            http("Get project details by project name: $projectName")
                .get("/managementportal/api/projects/$projectName")
                .headers(headers_http_authenticated_for_mp_admin)
                .check(
                    status().within(200, 404),
                    jsonPath("$.projectName").withDefault("").saveAs("projectName"),
                    jsonPath("$").withDefault("").saveAs("projectDTO"),
                )
        )
        .doIf({ session -> session.getString("projectName")?.isEmpty() }).then(
            exec(http("Create project")
                .post("/managementportal/api/projects")
                .headers(headers_http_authenticated_for_mp_admin)
                .body(StringBody(::projectBody))
                .check(status().shouldBe(201))
                .check(jsonPath("$").ofString().saveAs("projectDTO"))
            )
        ).exitHereIfFailed()
        .exec({ session ->
            // Store the project DTO for later use
            projectDto = session.getString("projectDTO")
            session
        })

    val ensureSourceTypes =
        exec(
            http("Get source types")
                .get("/managementportal/api/source-types")
                .headers(headers_http_authenticated_for_mp_admin)
                .check(
                    status().within(200),
                    jsonPath("$..[?(@.name == 'RADAR_aRMT')].name").withDefault("").saveAs("armt_source_id"),
                    jsonPath("$..[?(@.name == 'RADAR_pRMT')].name").withDefault("").saveAs("prmt_source_id"),
                    jsonPath("$..[?(@.name == 'ANDROID_PHONE')].name").withDefault("").saveAs("android_source_id"),
                    jsonPath("$..[?(@.name == 'RADAR_aRMT')]").withDefault("").saveAs("armt_source_dto"),
                    jsonPath("$..[?(@.name == 'RADAR_pRMT')]").withDefault("").saveAs("prmt_source_dto"),
                    jsonPath("$..[?(@.name == 'ANDROID_PHONE')]").withDefault("").saveAs("android_source_dto"),
                )
        )
        .doIf({ session -> session.getString("armt_source_id")?.isEmpty() }).then(
            exec(http("Create aRMT source type")
                .post("/managementportal/api/source-types")
                .headers(headers_http_authenticated_for_mp_admin)
                .body(StringBody(::aRmtSourceTypeBody)).asJson()
                .check(
                    status().shouldBe(201),
                    jsonPath("$.name").ofString().saveAs("armt_source_id"),
                    jsonPath("$").ofString().saveAs("armt_source_dto"),
                )
            ).exitHereIfFailed()
        )
        .doIf({ session -> session.getString("prmt_source_id")?.isEmpty() }).then(
            exec(http("Create pRMT source type")
                .post("/managementportal/api/source-types")
                .headers(headers_http_authenticated_for_mp_admin)
                .body(StringBody(::pRmtSourceTypeBody)).asJson()
                .check(
                    status().shouldBe(201),
                    jsonPath("$.name").ofString().saveAs("prmt_source_id"),
                    jsonPath("$").ofString().saveAs("prmt_source_dto"),
                )
            ).exitHereIfFailed()
        )
        .doIf({ session -> session.getString("android_source_id")?.isEmpty() }).then(
            exec(http("Create Android source type")
                .post("/managementportal/api/source-types")
                .headers(headers_http_authenticated_for_mp_admin)
                .body(StringBody(::androidSourceTypeBody)).asJson()
                .check(
                    status().shouldBe(201),
                    jsonPath("$.name").ofString().saveAs("android_source_id"),
                    jsonPath("$").ofString().saveAs("android_source_dto"),
                )
            ).exitHereIfFailed()
        )
        .exec { session: Session ->
            sourceTypeDtos["aRMT"] = session.getString("armt_source_dto").toString()
            sourceTypeDtos["pRMT"] = session.getString("prmt_source_dto").toString()
            sourceTypeDtos["android"] = session.getString("android_source_dto").toString()
            session
        }

    val ensureProjectSources =
        exec { session: Session ->
            val externalId = session.getString("externalId")
            session.setAll(
                mapOf(
                    "armtProjectSourceName" to projectSourceName("aRMT", externalId!!),
                    "prmtProjectSourceName" to projectSourceName("pRMT", externalId!!),
                    "androidProjectSourceName" to projectSourceName("android", externalId!!),
                )
            )
        }
        .exec(
            http("Get project sources")
                .get("/managementportal/api/sources")
                .headers(headers_http_authenticated_for_mp_admin)
                .check(
                    status().within(200),
                    jsonPath { session ->
                        "$..[?(@.sourceName == '${projectSourceName("aRMT", session.getString("externalId")!!)}')]"
                    }.withDefault("").saveAs("armt_project_source_dto"),
                    jsonPath { session ->
                        "$..[?(@.sourceName == '${projectSourceName("aRMT", session.getString("externalId")!!)}')].sourceId"
                    }.withDefault("").saveAs("armt_project_source_id"),
                    jsonPath { session ->
                        "$..[?(@.sourceName == '${projectSourceName("pRMT", session.getString("externalId")!!)}')]"
                    }.withDefault("").saveAs("prmt_project_source_dto"),
                    jsonPath { session ->
                        "$..[?(@.sourceName == '${projectSourceName("pRMT", session.getString("externalId")!!)}')].sourceId"
                    }.withDefault("").saveAs("prmt_project_source_id"),
                    jsonPath { session ->
                        "$..[?(@.sourceName == '${projectSourceName("android", session.getString("externalId")!!)}')]"
                    }.withDefault("").saveAs("android_project_source_dto"),
                    jsonPath { session ->
                        "$..[?(@.sourceName == '${projectSourceName("android", session.getString("externalId")!!)}')].sourceId"
                    }.withDefault("").saveAs("android_project_source_id"),
                )
        )
        .doIf({ session: Session -> session.getString("armt_project_source_id")?.isEmpty() }).then(
            exec(
                http("Create aRMT project source")
                    .post("/managementportal/api/sources")
                    .body(StringBody(::projectSourceBodyaRMT)).asJson()
                    .headers(headers_http_authenticated_for_mp_admin)
                    .check(
                        status().shouldBe(201),
                        jsonPath("$").ofString().saveAs("armt_project_source_dto"),
                        jsonPath("$.sourceId").ofString().saveAs("armt_project_source_id"),
                    )
            ).exitHereIfFailed()
        )
        .doIf({ session: Session -> session.getString("prmt_project_source_id")?.isEmpty() }).then(
            exec(
                http("Create pRMT project source")
                    .post("/managementportal/api/sources")
                    .body(StringBody(::projectSourceBodypRMT)).asJson()
                    .headers(headers_http_authenticated_for_mp_admin)
                    .check(
                        status().shouldBe(201),
                        jsonPath("$").ofString().saveAs("prmt_project_source_dto"),
                        jsonPath("$.sourceId").ofString().saveAs("prmt_project_source_id"),
                    )
            ).exitHereIfFailed()
        )
        .doIf({ session: Session -> session.getString("android_project_source_id")?.isEmpty() }).then(
            exec(
                http("Create Android project- source")
                    .post("/managementportal/api/sources")
                    .body(StringBody(::projectSourceBodyAndroid)).asJson()
                    .headers(headers_http_authenticated_for_mp_admin)
                    .check(
                        status().shouldBe(201),
                        jsonPath("$").ofString().saveAs("android_project_source_dto"),
                        jsonPath("$.sourceId").ofString().saveAs("android_project_source_id"),
                    )
            ).exitHereIfFailed()
        )
        .exec({ session: Session ->
            val armtProjectSourceName = projectSourceName("aRMT", session.getString("externalId")!!)
            val prmtProjectSourceName = projectSourceName("pRMT", session.getString("externalId")!!)
            val androidProjectSourceName = projectSourceName("android", session.getString("externalId")!!)
            projectSourceDtos[armtProjectSourceName] = session.getString("armt_project_source_dto").toString()
            projectSourceIds[armtProjectSourceName] = session.getString("armt_project_source_id").toString()
            projectSourceDtos[prmtProjectSourceName] = session.getString("prmt_project_source_dto").toString()
            projectSourceIds[prmtProjectSourceName] = session.getString("prmt_project_source_id").toString()
            projectSourceDtos[androidProjectSourceName] = session.getString("android_project_source_dto").toString()
            projectSourceIds[androidProjectSourceName] = session.getString("android_project_source_id").toString()
            session
        })

    val ensureSubject =
        exec(
            http("Get subject")
                .get("/managementportal/api/subjects")
                    .queryParam("externalId", "#{externalId}")
                .headers(headers_http_authenticated_for_mp_admin)
                .check(
                    jsonPath("$[0].login").withDefault("").saveAs("login")
                )
        )
        .doIf({ session: Session -> session.getString("login")?.isEmpty() }).then(
            exec(http("Create new subject")
                .post("/managementportal/api/subjects")
                .headers(headers_http_authenticated_for_mp_admin)
                .body(StringBody(::subjectReqBody)).asJson()
                .check(status().shouldBe(201))
                .check(jsonPath("$.login").ofString().saveAs("login"))
            ).exitHereIfFailed()
        )

    private val projectSetup = scenario("Setup organization and project")
        .exec(http("Authentication")
            .post("/managementportal/oauth/token")
            .headers(headers_http_authentication)
            .formParam("grant_type", "password")
            .formParam("client_id", clientId)
            .formParam("username", mpAdminUsername)
            .formParam("password", mpAdminPassword)
            .check(jsonPath("$.access_token").saveAs("mp_access_token"))).exitHereIfFailed()
        .pause(1)
        .exec(ensureOrganization)
        .pause(1)
        .exec(ensureSourceTypes)
        .pause(1)
        .exec(ensureProject)

    private val subjectRegistration = scenario("Register subjects")
        .exec(http("Authentication")
            .post("/managementportal/oauth/token")
            .headers(headers_http_authentication)
            .formParam("grant_type", "password")
            .formParam("client_id", clientId)
            .formParam("username", mpAdminUsername)
            .formParam("password", mpAdminPassword)
            .check(jsonPath("$.access_token").saveAs("mp_access_token"))).exitHereIfFailed()
        .foreach(userDetailsFeeder.readRecords().subList(0, numberOfParticipants), "subject", "counter").on(
            exec({ session: Session ->
                session.set("externalId", session.getMap<String>("subject")["externalId"])
            })
            .exec(ensureProjectSources)
            .exec(ensureSubject)
            .exec(http("PairApp - Get meta token")
                .get("/managementportal/api/oauth-clients/pair")
                .queryParam("clientId", clientIdToPair)
                .queryParam("login", "#{login}")
                .queryParam("persistent", "true")
                .headers(headers_http_authenticated_for_mp_admin)
                .check(status().shouldBe(200))
                .check(jsonPath("$.tokenName").ofString().saveAs("metaTokenForSubject"))).exitHereIfFailed()
            .exec(http("PairApp - Get refresh token")
                .get("/managementportal/api/meta-token/#{metaTokenForSubject}")
                .headers(headers_http_authenticated_for_mp_admin)
                .check(status().shouldBe(200))
                .check(jsonPath("$.refreshToken").ofString().saveAs("refreshTokenForSubject"))).exitHereIfFailed()
            .exec(http("Request Token for App")
                .post("/managementportal/oauth/token")
                .queryParam("grant_type", "refresh_token")
                .queryParam("refresh_token", "#{refreshTokenForSubject}")
                .headers(headers_http_authenticated_for_client)
                .check(jsonPath("$.access_token").saveAs("access_token_for_subject"))
                .check(jsonPath("$.refresh_token").ofString().saveAs("refreshTokenForSubject"))).exitHereIfFailed()
            .exec(http("Renew Token for App")
                .post("/managementportal/oauth/token")
                .queryParam("grant_type", "refresh_token")
                .queryParam("refresh_token", "#{refreshTokenForSubject}")
                .headers(headers_http_authenticated_for_client)
                .check(jsonPath("$.access_token").saveAs("access_token_for_subject"))
                .check(jsonPath("$.refresh_token").ofString().saveAs("refreshTokenForSubject"))).exitHereIfFailed()
            // Finally, store the access token for data ingestion simulation
            .exec({ session: Session ->
                externalIdToLogin[session.getString("externalId").toString()] = session.getString("login").toString()
                loginToAccessToken[session.getString("login").toString()] = session.getString("access_token_for_subject").toString()
                session
            })
        ).exitHereIfFailed()

    private val kafkaTopics = scenario("Fetch Kafka topics")
        .feed(loginFeeder)
        .exec { session : Session ->
            session.set("access_token_for_subject", loginToAccessToken[session.getString("login")])
        }
        .foreach(topics, "topic", "counter").on(
            exec(http("Check topic exists")
                .get("/kafka/topics/#{topic.topic}")
                .headers(headers_http_authenticated_for_subject)
                .check(status().shouldBe(200))
            ).exitHereIfFailed()
            .exec(http("GetKeySchemaId")
                .get("/schema/subjects/#{topic.topic}-key/versions/1")
                .headers(headers_http_authenticated_for_subject)
                .check(jsonPath("$.id").ofInt().saveAs("keySchemaId"))
            ).exitHereIfFailed()
            .exec { session : Session ->
                val topicName = session.getMap<String>("topic")["topic"].toString()
                keySchemaIds[topicName] = session.getInt("keySchemaId")
                session
            }
            .exec(http("GetValueSchemaId")
                .get("/schema/subjects/#{topic.topic}-value/versions/1")
                .headers(headers_http_authenticated_for_subject)
                .check(jsonPath("$.id").ofInt().saveAs("valueSchemaId"))
            ).exitHereIfFailed()
            .exec { session : Session ->
                val topicName = session.getMap<String>("topic")["topic"].toString()
                valueSchemaIds[topicName] = session.getInt("valueSchemaId")
                session
            }
        )

    private val sendJsonData = exec(
        http("Send Json Questionnaire Data")
            .post("/kafka/topics/#{topic}")
            .header("Content-Type", "application/vnd.kafka.json.v$kafkaApiVersion+json; charset=utf-8")
            .headers(headers_http_authenticated_for_subject)
            .body(StringBody(::avroRequestJsonBody)).asJson()
    ).exitHereIfFailed()

    private val sendBinaryData = exec(
        http("Send Json Questionnaire Data")
            .post("/kafka/topics/#{topic}")
            .header("Content-Type", "application/vnd.radarbase.avro.v$kafkaApiVersion+binary")
            .headers(headers_http_authenticated_for_subject)
            .body(ByteArrayBody(::avroRequestBinaryBody))
    ).exitHereIfFailed()


    private val dataIngestionQuestionnaireResponse = scenario("Upload questionnaire response data")
        .feed(loginFeeder)
        .exec { session : Session ->
            val login = session.getString("login")
            val projectSourceName = projectSourceName("aRMT", session.getString("externalId")!!)
            val sourceId = projectSourceIds[projectSourceName]
            val token = loginToAccessToken[login]
            session.setAll(
                mapOf(
                    "topic" to "questionnaire_response",
                    "valueClass" to "org.radarcns.active.questionnaire.Questionnaire",
                    "access_token_for_subject" to token,
                    "sourceId" to sourceId,
                )
            )
        }.exitHereIfFailed()
        .exec(sendJsonData)

    init {
        setUp(
            projectSetup.injectOpen(atOnceUsers(1)) // single admin setting up the project
                .andThen(
                    subjectRegistration.injectOpen(atOnceUsers(1)) // number of admins each registering all single subject. Needs to be the max number of users used below
                )
                .andThen(
                    kafkaTopics.injectOpen(atOnceUsers(1)) // single user setting up the topics
                )
//                .andThen(
//                    dataIngestionQuestionnaireResponse.injectOpen(atOnceUsers(1)) // single user sending data
//                )
                .andThen(
                    dataIngestionQuestionnaireResponse.injectClosed(
                        incrementConcurrentUsers(5)
                            .times(5)
                            .eachLevelLasting(60)
                            .separatedByRampsLasting(10)
                            .startingFrom(5)
                    )
                )
        ).protocols(httpConf)
    }

    private fun subjectReqBody(session: Session): String {
        val writer = StringWriter()
        val gen = JsonFactory().createGenerator(writer)
        gen.writeStartObject()
        gen.writeRaw("\"project\" : $projectDto,")
        gen.writeStringField("externalId", session.getString("externalId"))
        gen.writeNumberField("status", 1)
        gen.writeStringField("group", null)
        gen.writeArrayFieldStart("sources")
        gen.writeRaw("${session.getString("armt_project_source_dto")}, ")
        gen.writeRaw("${session.getString("prmt_project_source_dto")}, ")
        gen.writeRaw(session.getString("android_project_source_dto"))
        gen.writeEndArray()
        gen.writeEndObject()
        gen.flush()
        return writer.toString()
    }

    @Suppress("unused")
    private fun sourceReqBody(session: Session): String {
        val writer = StringWriter()
        val gen = JsonFactory().createGenerator(writer)
        gen.writeStartObject()
        gen.writeStringField("sourceTypeCatalogVersion", sourceTypeCatalogVersion)
        gen.writeStringField("sourceTypeModel", sourceTypeModel)
        gen.writeStringField("sourceTypeProducer", sourceTypeProducer)
        gen.writeEndObject()
        gen.flush()
        return writer.toString()
    }

    @Suppress("unused")
    private fun aRmtSourceTypeBody(session: Session): String {
        val writer = StringWriter()
        val gen = JsonFactory().createGenerator(writer)
        gen.writeStartObject()
        gen.writeStringField("appProvider", "org.radarcns.application.ApplicationServiceProvider")
        gen.writeStringField("producer", "RADAR")
        gen.writeStringField("model", "aRMT")
        gen.writeStringField("catalogVersion", "1.5.0")
        gen.writeStringField("canRegisterDynamically", "true")
        gen.writeStringField("sourceTypeScope", "ACTIVE")
        gen.writeStringField("name", "RADAR_aRMT")
        gen.writeEndObject()
        gen.flush()
        return writer.toString()
    }

    @Suppress("unused")
    private fun pRmtSourceTypeBody(session: Session): String {
        val writer = StringWriter()
        val gen = JsonFactory().createGenerator(writer)
        gen.writeStartObject()
        gen.writeStringField("appProvider", "org.radarcns.application.ApplicationServiceProvider")
        gen.writeStringField("producer", "RADAR")
        gen.writeStringField("model", "pRMT")
        gen.writeStringField("catalogVersion", "1.1.0")
        gen.writeStringField("canRegisterDynamically", "true")
        gen.writeStringField("sourceTypeScope", "PASSIVE")
        gen.writeStringField("name", "RADAR_pRMT")
        gen.writeEndObject()
        gen.flush()
        return writer.toString()
    }

    private fun androidSourceTypeBody(session: Session): String {
        val writer = StringWriter()
        val gen = JsonFactory().createGenerator(writer)
        gen.writeStartObject()
        gen.writeStringField("producer", "ANDROID")
        gen.writeStringField("model", "PHONE")
        gen.writeStringField("catalogVersion", "1.0.0")
        gen.writeStringField("canRegisterDynamically", "true")
        gen.writeStringField("sourceTypeScope", "PASSIVE")
        gen.writeStringField("name", "ANDROID_PHONE")
        gen.writeEndObject()
        gen.flush()
        return writer.toString()
    }

    @Suppress("unused")
    private fun organizationBody(session: Session): String {
        val writer = StringWriter()
        val gen = JsonFactory().createGenerator(writer)
        gen.writeStartObject()
        gen.writeStringField("name", organizationName)
        gen.writeStringField("description", "Test Organization")
        gen.writeStringField("location", "Test Location")
        gen.writeEndObject()
        gen.flush()
        return writer.toString()
    }

    @Suppress("unused")
    private fun projectBody(session: Session): String {
        val writer = StringWriter()
        val gen = JsonFactory().createGenerator(writer)
        gen.writeStartObject()
        gen.writeRaw("\"organization\" : " + session.getString("organizationDTO") + ",")
        gen.writeStringField("projectName", projectName)
        gen.writeStringField("description", "Test Project")
        gen.writeStringField("location", "Test Location")
        gen.writeArrayFieldStart("sourceTypes")
        gen.writeRaw(session.getString("armt_source_dto") + ",")
        gen.writeRaw(session.getString("prmt_source_dto") + ",")
        gen.writeRaw(session.getString("android_source_dto"))
        gen.writeEndArray()
        gen.writeEndObject()
        gen.flush()
        return writer.toString()
    }

    private fun projectSourceBodyaRMT(session: Session) = projectSourceBody(session, "aRMT", sourceTypeDtos["aRMT"])
    private fun projectSourceBodypRMT(session: Session) = projectSourceBody(session, "pRMT", sourceTypeDtos["pRMT"])
    private fun projectSourceBodyAndroid(session: Session) = projectSourceBody(session, "android", sourceTypeDtos["android"])

    @Suppress("unused")
    private fun projectSourceBody(session: Session, sourceTypeName: String, sourceTypeDto: String?): String {
        val projectSourceName = projectSourceName(sourceTypeName, session.getString("externalId")!!)
        val writer = StringWriter()
        val gen = JsonFactory().createGenerator(writer)
        gen.writeStartObject()
        gen.writeRaw("\"project\" : $projectDto,")
        gen.writeRaw("\"sourceType\" : $sourceTypeDto,")
        gen.writeStringField("sourceName", projectSourceName)
        gen.writeBooleanField("assigned", false)
        gen.writeEndObject()
        gen.flush()
        return writer.toString()
    }

//    private fun avroRequestBodyJson(session: Session): String {
//        val valueClass = Class.forName(session.getString("valueClass"))
//        val valueSchema = schema(valueClass)
//        val recordCount = session.getInt("recordCount")
//        val records = RandomData(valueSchema, recordCount)
//        val keyId = keySchemaIds[session.getString("topic")]!!
//        val valueId = valueSchemaIds[session.getString("topic")]!!
//        val writer = StringWriter()
//        val gen = JsonFactory().createGenerator(writer)
//        gen.writeStartObject()
//        gen.writeNumberField("key_schema_id", keyId)
//        gen.writeNumberField("value_schema_id", valueId)
//        gen.writeArrayFieldStart("records")
//        var first = true
//        records
//            .map { it as GenericData.Record }
//            .map {
//                val now = System.currentTimeMillis() / 1000.0
//                it.put("time", now)
//                if (!first) {
//                    gen.writeRaw(", ")
//                }
//                first = false
//                gen.writeRaw("{ \"key\": {"
//                        + "\"projectId\": {\"string\":\"$projectName\"},"
//                        + "\"userId\": \"" + session.getString("login") + "\","
//                        + "\"sourceId\": \"" + session.getString("sourceId") + "\""
//                        + "}"
//                        + ", \"value\": " + toJson(it) + "}")
//            }
//        gen.writeEndArray()
//        gen.writeEndObject()
//        gen.flush()
//        return writer.toString()
//    }

    private fun avroRequestJsonBody(session: Session): String {
        val encoder = RemoteSchemaEncoder(binary = false)
        val keyWriter = encoder.writer(ObservationKey.getClassSchema(), ObservationKey::class.java  )
        val valueWriter = encoder.writer(Questionnaire.getClassSchema(), Questionnaire::class.java)
        val key = keyWriter.encode(
            ObservationKey(
                projectName, session.getString("login"), session.getString("sourceId")
            )
        )
        val questionnaires = List(messageNumber) { valueWriter.encode(createQuestionnaireValue(messageSize)) }
        val keyId = keySchemaIds[session.getString("topic")]!!
        val valueId = valueSchemaIds[session.getString("topic")]!!
        val writer = StringWriter()
        val gen = JsonFactory().createGenerator(writer)
        gen.writeStartObject()
            gen.writeNumberField("key_schema_id", keyId)
            gen.writeNumberField("value_schema_id", valueId)
            gen.writeArrayFieldStart("records")
            questionnaires.forEach {
                gen.writeStartObject()
                gen.writeRaw("\"key\": ${String(key)} ,")
                gen.writeRaw("\"value\": ${String(it)}")
                gen.writeEndObject()
            }
            gen.writeEndArray()
        gen.writeEndObject()
        gen.flush()
        return writer.toString()
    }

    private fun avroRequestBinaryBody(session: Session): ByteArray {
        val keyId = keySchemaIds[session.getString("topic")]!!
        val valueId = valueSchemaIds[session.getString("topic")]!!
//        val keyParsedSchemaMetadata = ParsedSchemaMetadata(keyId, 1, ObservationKey.getClassSchema())
        val valueParsedSchemaMetadata = ParsedSchemaMetadata(valueId, 1, Questionnaire.getClassSchema())
        val valueWriter = RemoteSchemaEncoder.SchemaEncoderWriter(
            binary = true,
            schema = valueParsedSchemaMetadata.schema,
            clazz = Questionnaire::class.java,
            readerSchema = valueParsedSchemaMetadata.schema
        )
        val value = valueWriter.encode(createQuestionnaireValue(messageSize))
        val recordSet = RecordSet(
            1,
            1,
            projectName,
            session.getString("login"),
            session.getString("sourceId"),
            listOf(ByteBuffer.wrap(value))
        )
        val byteBuffer = recordSet.toByteBuffer()
        val byteArray = ByteArray(byteBuffer.capacity())
        byteBuffer.get(byteArray)
//        val b: ByteBuffer = ByteBuffer.wrap(byteArray)
        println(String(byteArray))
        return byteArray
    }

    private fun createQuestionnaireValue(numAnswers: Int): Questionnaire {
        var localTime = (System.currentTimeMillis() / 1000.0) - random.nextInt(10000)
        val answers = List(numAnswers) { index ->
            localTime += 1000
            Answer("questionId", "Answer #$index", localTime, localTime)
        }
        localTime += 1000
        return Questionnaire(localTime, localTime, localTime, "test", "1", answers)
    }

    private fun projectSourceName(sourceTypeName: String, externalId: String) = "$sourceTypeName-source-$externalId"

}