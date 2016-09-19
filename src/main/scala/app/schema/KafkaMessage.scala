package app.schema

case class KafkaMessage(topic: String,
                        offset: Long,
                        key: String,
                        message: String)
