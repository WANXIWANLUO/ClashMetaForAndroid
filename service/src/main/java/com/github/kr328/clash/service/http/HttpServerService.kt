package com.github.kr328.clash.service.http

import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.IBinder
import com.github.kr328.clash.common.log.Log
import com.github.kr328.clash.common.constants.Intents
import com.github.kr328.clash.service.ProfileManager
import com.github.kr328.clash.service.ProfileProcessor
import com.github.kr328.clash.service.model.Profile
import com.github.kr328.clash.service.remote.IProfileManager
import com.github.kr328.clash.service.store.ServiceStore
import com.github.kr328.clash.service.util.pendingDir
import kotlinx.coroutines.*
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.URLDecoder
import java.util.*

class HttpServerService : Service(), CoroutineScope by CoroutineScope(Dispatchers.IO) {
    private var serverSocket: ServerSocket? = null
    private var serverJob: Job? = null
    private val store by lazy { ServiceStore(this) }
    private val profileManager by lazy { ProfileManager(this) }

    companion object {
        private const val DEFAULT_PORT = 9090
        private const val TAG = "HttpApiServer"

        fun start(context: Context) {
            context.startService(Intent(context, HttpServerService::class.java))
        }

        fun stop(context: Context) {
            context.stopService(Intent(context, HttpServerService::class.java))
        }
    }

    override fun onCreate() {
        super.onCreate()
        Log.d(TAG, "HTTP API Server creating...")
        startServer()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        return START_STICKY
    }

    override fun onDestroy() {
        super.onDestroy()
        stopServer()
        cancel()
        Log.d(TAG, "HTTP API Server destroyed")
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun startServer() {
        serverJob = launch {
            try {
                val port = store.httpApiPort.takeIf { it in 1024..65535 } ?: DEFAULT_PORT
                serverSocket = ServerSocket(port)
                Log.d(TAG, "HTTP API Server started on port $port")

                while (isActive) {
                    try {
                        val client = serverSocket?.accept() ?: continue
                        launch { handleClient(client) }
                    } catch (e: Exception) {
                        if (isActive) {
                            Log.e(TAG, "Error accepting client: ${e.message}", e)
                        }
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "Server error: ${e.message}", e)
            }
        }
    }

    private fun stopServer() {
        serverJob?.cancel()
        try {
            serverSocket?.close()
        } catch (e: Exception) {
            Log.e(TAG, "Error closing server: ${e.message}", e)
        }
    }

    private suspend fun handleClient(client: Socket) {
        try {
            val input = BufferedReader(InputStreamReader(client.getInputStream()))
            val output = client.getOutputStream()
            
            val requestLine = input.readLine() ?: return
            val parts = requestLine.split(" ")
            if (parts.size < 3) {
                sendResponse(output, 400, "Bad Request")
                client.close()
                return
            }

            val method = parts[0]
            val fullPath = parts[1]
            
            val queryStart = fullPath.indexOf('?')
            val path = if (queryStart >= 0) fullPath.substring(0, queryStart) else fullPath
            val queryString = if (queryStart >= 0) fullPath.substring(queryStart + 1) else ""
            val queryParams = parseQueryParams(queryString)
            
            val headers = mutableMapOf<String, String>()
            var line: String?
            while (input.readLine().also { line = it } != null && line?.isNotEmpty() == true) {
                val colonIndex = line?.indexOf(':') ?: -1
                if (colonIndex > 0) {
                    val key = line?.substring(0, colonIndex)?.trim() ?: continue
                    val value = line?.substring(colonIndex + 1)?.trim() ?: continue
                    headers[key] = value
                }
            }

            val contentLength = headers["Content-Length"]?.toIntOrNull() ?: 0
            val body = if (contentLength > 0) {
                val buffer = CharArray(contentLength)
                input.read(buffer, 0, contentLength)
                String(buffer)
            } else {
                ""
            }

            val response = handleRequest(method, path, body, queryParams)
            sendResponse(output, response.first, response.second)
            
            client.close()
        } catch (e: Exception) {
            Log.e(TAG, "Error handling client: ${e.message}", e)
            try {
                client.close()
            } catch (e: Exception) {
            }
        }
    }

    private fun parseQueryParams(query: String): Map<String, String> {
        if (query.isEmpty()) return emptyMap()
        return query.split("&").mapNotNull { param ->
            val parts = param.split("=", 2)
            if (parts.size == 2) {
                parts[0] to URLDecoder.decode(parts[1], "UTF-8")
            } else null
        }.toMap()
    }

    private suspend fun handleRequest(method: String, path: String, body: String, queryParams: Map<String, String>): Pair<Int, String> {
        return when {
            path == "/api/v1/start" && method == "POST" -> handleStart()
            path == "/api/v1/stop" && method == "POST" -> handleStop()
            path == "/api/v1/toggle" && method == "POST" -> handleToggle()
            path == "/api/v1/status" && method == "GET" -> handleStatus()
            path == "/api/v1/profiles" && method == "GET" -> handleListProfiles()
            path == "/api/v1/profiles" && method == "POST" -> handleCreateProfile(body)
            path == "/api/v1/profiles/import" && method == "POST" -> handleImportProfile(body, queryParams)
            path.startsWith("/api/v1/profiles/") && method == "DELETE" -> handleDeleteProfile(path)
            path.startsWith("/api/v1/profiles/") && method == "GET" && !path.endsWith("/import") -> handleGetProfile(path)
            path.startsWith("/api/v1/profiles/") && method == "PUT" -> handleSetActiveProfile(path)
            path == "/api/v1/port" && method == "POST" -> handleSetPort(body)
            path == "/api/v1/help" && method == "GET" -> handleHelp()
            else -> 404 to "{\"error\":\"Not Found\",\"help\":\"Try /api/v1/help\"}"
        }
    }

    private fun handleStart(): Pair<Int, String> {
        Log.d(TAG, "Received start request")
        return try {
            val intent = Intent(Intents.ACTION_START_CLASH).apply {
                setPackage(packageName)
            }
            sendBroadcast(intent)
            200 to "{\"success\":true,\"message\":\"Starting Clash\"}"
        } catch (e: Exception) {
            Log.e(TAG, "Error starting: ${e.message}", e)
            500 to "{\"success\":false,\"message\":\"${e.message}\"}"
        }
    }

    private fun handleStop(): Pair<Int, String> {
        Log.d(TAG, "Received stop request")
        return try {
            val intent = Intent(Intents.ACTION_STOP_CLASH).apply {
                setPackage(packageName)
            }
            sendBroadcast(intent)
            200 to "{\"success\":true,\"message\":\"Stopping Clash\"}"
        } catch (e: Exception) {
            Log.e(TAG, "Error stopping: ${e.message}", e)
            500 to "{\"success\":false,\"message\":\"${e.message}\"}"
        }
    }

    private fun handleToggle(): Pair<Int, String> {
        Log.d(TAG, "Received toggle request")
        return try {
            val intent = Intent(Intents.ACTION_TOGGLE_CLASH).apply {
                setPackage(packageName)
            }
            sendBroadcast(intent)
            200 to "{\"success\":true,\"message\":\"Toggling Clash\"}"
        } catch (e: Exception) {
            Log.e(TAG, "Error toggling: ${e.message}", e)
            500 to "{\"success\":false,\"message\":\"${e.message}\"}"
        }
    }

    private fun handleStatus(): Pair<Int, String> {
        Log.d(TAG, "Received status request")
        val isRunning = com.github.kr328.clash.service.StatusProvider.serviceRunning
        val activeProfile = try {
            runBlocking { profileManager.queryActive() }
        } catch (e: Exception) {
            null
        }
        
        return 200 to """
            {
                "success": true,
                "running": $isRunning,
                "activeProfile": ${activeProfile?.let { 
                    """{"uuid":"${it.uuid}","name":"${it.name}","type":"${it.type}"}"""
                } ?: "null"}
            }
        """.trimIndent()
    }

    private suspend fun handleListProfiles(): Pair<Int, String> {
        Log.d(TAG, "Received list profiles request")
        return try {
            val profiles = profileManager.queryAll()
            val json = profiles.joinToString(",", "[", "]") { p ->
                """{"uuid":"${p.uuid}","name":"${p.name}","type":"${p.type}","active":${p.active},"imported":${p.imported}}"""
            }
            200 to """{"success":true,"profiles":$json}"""
        } catch (e: Exception) {
            Log.e(TAG, "Error listing profiles: ${e.message}", e)
            500 to """{"success":false,"message":"${e.message}"}"""
        }
    }

    private suspend fun handleCreateProfile(body: String): Pair<Int, String> {
        Log.d(TAG, "Received create profile request")
        return try {
            val name = extractJsonString(body, "name") ?: "New Profile"
            val typeStr = extractJsonString(body, "type") ?: "url"
            val source = extractJsonString(body, "source") ?: ""
            
            val type = when (typeStr.lowercase()) {
                "file" -> Profile.Type.File
                else -> Profile.Type.Url
            }
            
            val uuid = profileManager.create(type, name, source)
            200 to """{"success":true,"uuid":"$uuid","message":"Profile created"}"""
        } catch (e: Exception) {
            Log.e(TAG, "Error creating profile: ${e.message}", e)
            500 to """{"success":false,"message":"${e.message}"}"""
        }
    }

    private suspend fun handleImportProfile(body: String, queryParams: Map<String, String>): Pair<Int, String> {
        Log.d(TAG, "Received import profile request")
        return try {
            val url = extractJsonString(body, "url") ?: queryParams["url"] ?: 
                      queryParams["config"] ?: queryParams["source"] ?: ""
            val name = extractJsonString(body, "name") ?: queryParams["name"] ?: 
                      "Imported ${System.currentTimeMillis()}"
            val autoSelect = queryParams["auto"]?.lowercase() == "true" || 
                            queryParams["autoSelect"]?.lowercase() == "true" ||
                            queryParams["activate"]?.lowercase() == "true" ||
                            queryParams["select"]?.lowercase() == "true" ||
                            extractJsonBoolean(body, "auto") == true
            
            if (url.isEmpty()) {
                return 400 to """{"success":false,"message":"URL is required"}"""
            }
            
            val uuid = profileManager.create(Profile.Type.Url, name, url)
            Log.d(TAG, "Profile created with UUID: $uuid")
            
            profileManager.commit(uuid, null)
            Log.d(TAG, "Profile committed")
            
            if (autoSelect) {
                ProfileProcessor.active(this, uuid)
                Log.d(TAG, "Profile $uuid set as active")
            }
            
            200 to """{
                "success": true,
                "uuid": "$uuid",
                "name": "$name",
                "url": "$url",
                "active": $autoSelect,
                "message": "Profile imported${if (autoSelect) " and activated" else ""}"
            }"""
        } catch (e: Exception) {
            Log.e(TAG, "Error importing profile: ${e.message}", e)
            500 to """{"success":false,"message":"${e.message}"}"""
        }
    }

    private suspend fun handleDeleteProfile(path: String): Pair<Int, String> {
        Log.d(TAG, "Received delete profile request")
        return try {
            val uuidStr = path.substringAfter("/api/v1/profiles/").substringBefore("/")
            val uuid = UUID.fromString(uuidStr)
            
            profileManager.delete(uuid)
            200 to """{"success":true,"uuid":"$uuid","message":"Profile deleted"}"""
        } catch (e: Exception) {
            Log.e(TAG, "Error deleting profile: ${e.message}", e)
            400 to """{"success":false,"message":"${e.message}"}"""
        }
    }

    private suspend fun handleGetProfile(path: String): Pair<Int, String> {
        Log.d(TAG, "Received get profile request")
        return try {
            val uuidStr = path.substringAfter("/api/v1/profiles/").substringBefore("/")
            val uuid = UUID.fromString(uuidStr)
            
            val profile = profileManager.queryByUUID(uuid)
            if (profile != null) {
                200 to """{
                    "success": true,
                    "profile": {
                        "uuid": "${profile.uuid}",
                        "name": "${profile.name}",
                        "type": "${profile.type}",
                        "source": "${profile.source}",
                        "active": ${profile.active},
                        "interval": ${profile.interval}
                    }
                }"""
            } else {
                404 to """{"success":false,"message":"Profile not found"}"""
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error getting profile: ${e.message}", e)
            400 to """{"success":false,"message":"${e.message}"}"""
        }
    }

    private suspend fun handleSetActiveProfile(path: String): Pair<Int, String> {
        Log.d(TAG, "Received set active profile request")
        return try {
            val uuidStr = path.substringAfter("/api/v1/profiles/").substringBefore("/")
            val uuid = UUID.fromString(uuidStr)
            
            val profile = profileManager.queryByUUID(uuid)
            if (profile != null) {
                ProfileProcessor.active(this, uuid)
                200 to """{"success":true,"uuid":"$uuid","name":"${profile.name}","message":"Profile activated"}"""
            } else {
                404 to """{"success":false,"message":"Profile not found"}"""
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error setting active profile: ${e.message}", e)
            400 to """{"success":false,"message":"${e.message}"}"""
        }
    }

    private fun handleSetPort(body: String): Pair<Int, String> {
        Log.d(TAG, "Received set port request")
        return try {
            val port = body.toIntOrNull() ?: return 400 to """{"success":false,"message":"Invalid port"}"""
            if (port !in 1024..65535) {
                return 400 to """{"success":false,"message":"Port must be between 1024-65535"}"""
            }
            store.httpApiPort = port
            Log.d(TAG, "HTTP API port set to $port, restart server to apply changes")
            200 to """{"success":true,"port":$port,"message":"Port set to $port, restart server to apply"}"""
        } catch (e: Exception) {
            500 to """{"success":false,"message":"${e.message}"}"""
        }
    }

    private fun handleHelp(): Pair<Int, String> {
        return 200 to """
            {
                "success": true,
                "api": "ClashMetaForAndroid HTTP API v1",
                "endpoints": [
                    {"method": "GET", "path": "/api/v1/help", "desc": "Show this help"},
                    {"method": "GET", "path": "/api/v1/status", "desc": "Get Clash running status"},
                    {"method": "POST", "path": "/api/v1/start", "desc": "Start Clash VPN"},
                    {"method": "POST", "path": "/api/v1/stop", "desc": "Stop Clash VPN"},
                    {"method": "POST", "path": "/api/v1/toggle", "desc": "Toggle Clash VPN state"},
                    {"method": "GET", "path": "/api/v1/profiles", "desc": "List all profiles"},
                    {"method": "POST", "path": "/api/v1/profiles", "desc": "Create a new profile", "body": "{\"name\":\"...\",\"type\":\"url|file\",\"source\":\"...\"}"},
                    {"method": "POST", "path": "/api/v1/profiles/import", "desc": "Import and activate config URL", "params": "url=...&name=...&auto=true"},
                    {"method": "GET", "path": "/api/v1/profiles/{uuid}", "desc": "Get profile details"},
                    {"method": "PUT", "path": "/api/v1/profiles/{uuid}", "desc": "Set profile as active"},
                    {"method": "DELETE", "path": "/api/v1/profiles/{uuid}", "desc": "Delete a profile"},
                    {"method": "POST", "path": "/api/v1/port", "desc": "Set HTTP API port", "body": "9090"}
                ],
                "examples": [
                    "curl -X POST 'http://localhost:9090/api/v1/start'",
                    "curl -X POST 'http://localhost:9090/api/v1/profiles/import?url=https://example.com/config.yaml&name=MyConfig&auto=true'",
                    "curl 'http://localhost:9090/api/v1/status'",
                    "curl -X PUT 'http://localhost:9090/api/v1/profiles/550e8400-e29b-41d4-a716-446655440000'"
                ]
            }
        """.trimIndent()
    }

    private fun extractJsonString(json: String, key: String): String? {
        val regex = """"$key"\s*:\s*"([^"]*)"""".toRegex()
        return regex.find(json)?.groupValues?.get(1)
    }

    private fun extractJsonBoolean(json: String, key: String): Boolean? {
        val regex = """"$key"\s*:\s*(true|false)""".toRegex()
        return regex.find(json)?.groupValues?.get(1)?.toBoolean()
    }

    private fun sendResponse(output: OutputStream, code: Int, message: String) {
        val statusText = when (code) {
            200 -> "OK"
            400 -> "Bad Request"
            404 -> "Not Found"
            500 -> "Internal Server Error"
            else -> "Unknown"
        }
        val response = """
            HTTP/1.1 $code $statusText
            Content-Type: application/json; charset=utf-8
            Access-Control-Allow-Origin: *
            Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS
            Access-Control-Allow-Headers: Content-Type
            Content-Length: ${message.toByteArray().size}
            
            $message
        """.trimIndent()
        
        output.write(response.toByteArray())
        output.flush()
    }
}
