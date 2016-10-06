package app.schema

import java.sql.Timestamp

case class JobMetrics(batchDate: String,
                      appName: String,
                      jobId: Int,
                      batchTime: Timestamp,
                      applicationId: String,
                      master: String,
                      jobInfo: String,
                      jobStartTime: Timestamp,
                      var jobEndTime: Timestamp = new Timestamp(0L),
                      var prcsgTime: Long = 0L)
