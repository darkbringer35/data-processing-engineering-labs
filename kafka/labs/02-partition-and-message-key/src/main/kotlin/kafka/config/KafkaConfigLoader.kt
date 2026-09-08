import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import lab.kafka.config.ApplicationConfig

object KafkaConfigLoader {

    private val mapper = ObjectMapper(YAMLFactory())
        .registerKotlinModule()
        .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)

    fun load(): ApplicationConfig {
        val resource = Thread.currentThread()
            .contextClassLoader
            .getResourceAsStream("application.yml")
            ?: error("application.yml not found")

        return resource.use {
            mapper.readValue(it, ApplicationConfig::class.java)
        }
    }
}