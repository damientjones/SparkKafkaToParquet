package app.schema

import java.sql.Timestamp

import app.annotation.DefaultValue

import scala.annotation.meta.field

case class BatchMetrics(batchDate: String,
                        appName: String,
                        batchTime: Timestamp,
                        var status: String,
                        applicationId: String,
                        master: String,
                        @(DefaultValue @field)("-1")
                        var schedDelay: Long = -1L,
                        @(DefaultValue @field)("-1")
                        var prcsgDelay: Long = -1L,
                        @(DefaultValue @field)("-1")
                        var totalDelay: Long = -1L,
                        @(DefaultValue @field)("-1")
                        var recordCount: Long = -1L,
                        @(DefaultValue @field)("-1")
                        var throughput: Long = -1L,
                        @(DefaultValue @field)("-1")
                        var totSchedDelayDly: Long = -1L,
                        @(DefaultValue @field)("-1")
                        var totPrcsgDelayDly: Long = -1L,
                        @(DefaultValue @field)("-1")
                        var totalDelayDly: Long = -1L,
                        @(DefaultValue @field)("-1")
                        var totRecsPrcsdDly: Long = -1L,
                        @(DefaultValue @field)("-1")
                        var avgThroughputDly: Long = -1L)