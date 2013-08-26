package kafka.utils

object Os {
    private val osName = System.getProperty("os.name").toLowerCase
    val isWindows = osName.startsWith("windows")
}
