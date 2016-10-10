package app.schema

import java.sql.Timestamp

import app.annotation.DefaultValue

import scala.annotation.meta.field

case class JobMetrics(batchDate: String,
                      appName: String,
                      jobId: Int,
                      batchTime: Timestamp,
                      applicationId: String,
                      master: String,
                      jobInfo: String,
                      jobStartTime: Timestamp,
                      @(DefaultValue@field)("1969-12-31 19:00:00.0")
                      var jobEndTime: Timestamp = new Timestamp(0L),
                      @(DefaultValue@field)("-1")
                      var prcsgTime: Long = -1L)
