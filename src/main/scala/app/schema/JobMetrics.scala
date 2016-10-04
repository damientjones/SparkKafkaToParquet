package app.schema

import java.sql.Timestamp

case class JobMetrics(date: String,
                      appName: String,
                      batchTime: Timestamp,
                      var status: String,
                      applicationId: String,
                      master: String,
                      var schedDelay: Long = 0L,
                      var prcsgDelay: Long = 0L,
                      var totalDelay: Long = 0L,
                      var recordCount: Long = 0L,
                      var throughput: Long = 0L,
                      var totSchedDelayDly: Long = 0L,
                      var totPrcsgDelayDly: Long = 0L,
                      var totalDelayDly: Long = 0L,
                      var totRecsPrcsdDly: Long = 0L,
                      var avgThroughputDly: Long = 0L)