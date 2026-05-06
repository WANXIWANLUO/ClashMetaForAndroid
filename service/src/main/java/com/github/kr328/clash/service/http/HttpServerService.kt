package com.github.kr328.clash.service.http

import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.IBinder
import com.github.kr328.clash.common.log.Log
import com.github.kr328.clash.common.constants.Intents
import com.github.kr328.clash.service.store.ServiceStore
import kotlinx.coroutines.*
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.URLDecoder

class HttpServerService : Service(), CoroutineScope by CoroutineScope(Dispatchers.IO) {
    private var serverSocket: ServerSocket? = null
    private var serverJob: Job? = null
    private val store by lazy { ServiceStore(this) }

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
            val path = parts[1]
            
            // Read headers
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

            // Read body if exists
            val contentLength = headers["Content-Length"]?.toIntOrNull() ?: 0
            val body = if (contentLength > 0) {
                val buffer = CharArray(contentLength)
                input.read(buffer, 0, contentLength)
                String(buffer)
            } else {
                ""
            }

            // Handle request
            val response = handleRequest(method, path, body)
            sendResponse(output, response.first, response.second)
            
            client.close()
        } catch (e: Exception) {
            Log.e(TAG, "Error handling client: ${e.message}", e)
            try {
                client.close()
            } catch (e: Exception) {
                // Ignore
            }
        }
    }

    private fun handleRequest(method: String, path: String, body: String): Pair<Int, String> {
        return when {
            path.startsWith("/api/v1/start") && method == "POST" -> handleStart()
            path.startsWith("/api/v1/stop") && method == "POST" -> handleStop()
            path.startsWith("/api/v1/toggle") && method == "POST" -> handleToggle()
            path.startsWith("/api/v1/status") && method == "GET" -> handleStatus()
            path.startsWith("/api/v1/rule") && method == "POST" -> handleAddRule(body)
            path.startsWith("/api/v1/port") && method == "POST" -> handleSetPort(body)
            else -> 404 to "Not Found"
        }
    }

    private fun handleStart(): Pair<Int, String> {
        Log.d(TAG, "Received start request")
        try {
            val intent = Intent(Intents.ACTION_START_CLASH).apply {
                setPackage(packageName)
            }
            sendBroadcast(intent)
            return 200 to "{\"success\":true,\"message\":\"Starting Clash\"}"
        } catch (e: Exception) {
            Log.e(TAG, "Error starting: ${e.message}", e)
            return 500 to "{\"success\":false,\"message\":\"${e.message}\"}"
        }
    }

    private fun handleStop(): Pair<Int, String> {
        Log.d(TAG, "Received stop request")
        try {
            val intent = Intent(Intents.ACTION_STOP_CLASH).apply {
                setPackage(packageName)
            }
            sendBroadcast(intent)
            return 200 to "{\"success\":true,\"message\":\"Stopping Clash\"}"
        } catch (e: Exception) {
            Log.e(TAG, "Error stopping: ${e.message}", e)
            return 500 to "{\"success\":false,\"message\":\"${e.message}\"}"
        }
    }

    private fun handleToggle(): Pair<Int, String> {
        Log.d(TAG, "Received toggle request")
        try {
            val intent = Intent(Intents.ACTION_TOGGLE_CLASH).apply {
                setPackage(packageName)
            }
            sendBroadcast(intent)
            return 200 to "{\"success\":true,\"message\":\"Toggling Clash\"}"
        } catch (e: Exception) {
            Log.e(TAG, "Error toggling: ${e.message}", e)
            return 500 to "{\"success\":false,\"message\":\"${e.message}\"}"
        }
    }

    private fun handleStatus(): Pair<Int, String> {
        Log.d(TAG, "Received status request")
        val isRunning = com.github.kr328.clash.service.StatusProvider.serviceRunning
        return 200 to "{\"success\":true,\"running\":$isRunning}"
    }

    private fun handleAddRule(body: String): Pair<Int, String> {
        Log.d(TAG, "Received add rule request")
        // This is a placeholder - actual rule management would require more complex handling
        return 200 to "{\"success\":true,\"message\":\"Rule addition not implemented yet\"}"
    }

    private fun handleSetPort(body: String): Pair<Int, String> {
        Log.d(TAG, "Received set port request")
        try {
            val port = body.toIntOrNull() ?: return 400 to "{\"success\":false,\"message\":\"Invalid port\"}"
            if (port !in 1024..65535) {
                return 400 to "{\"success\":false,\"message\":\"Port must be between 1024-65535\"}"
            }
            store.httpApiPort = port
            Log.d(TAG, "HTTP API port set to $port, restart server to apply changes")
            return 200 to "{\"success\":true,\"message\":\"Port set to $port, restart server to apply\"}"
        } catch (e: Exception) {
            return 500 to "{\"success\":false,\"message\":\"${e.message}\"}"
        }
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
            Content-Type: application/json
            Access-Control-Allow-Origin: *
            Access-Control-Allow-Methods: GET, POST, OPTIONS
            Access-Control-Allow-Headers: Content-Type
            Content-Length: ${message.length}
            
            $message
        """.trimIndent()
        
        output.write(response.toByteArray())
        output.flush()
    }
}
