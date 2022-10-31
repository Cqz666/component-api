package com.cqz.api

object FeatureType extends Enumeration {
  type FeatureType = Value
  val OFFLINE_DETAIL_FEATURE = Value(1,"OfflineDetailFeature")
  val OFFLINE_AGG_FEATURE = Value(2,"OfflineAggFeature")

  def main(args: Array[String]): Unit = {
    println(FeatureType.OFFLINE_DETAIL_FEATURE)
    println(FeatureType.OFFLINE_AGG_FEATURE)

    val value = FeatureType.withName("OfflineDetailFeature")
    println(value)

    val value1 = FeatureType(1)
    println(value1)

    println(FeatureType.OFFLINE_AGG_FEATURE.id)

  }
}
