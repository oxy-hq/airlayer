view: ad_impressions {
  sql_table_name: @{GOOGLE_ADS_SCHEMA}.ACCOUNT_HOURLY_STATS ;;

  dimension: primary_key {
    primary_key: yes
    type: string
    hidden: yes
    sql: CONCAT(${_date}, ${ad_network_type1}) ;;
  }

  dimension: _date {
    type: date_raw
    sql: CAST(${TABLE}.date AS DATE) ;;
    convert_tz: no
  }

  dimension_group: date {
    type: time
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
      day_of_week,
      day_of_week_index,
      day_of_month,
      day_of_year
    ]
    sql: ${TABLE}.date ;;
  }

  dimension: ad_network_type1 {
    type: string
    sql: ${TABLE}.ad_network_type1 ;;
    hidden: yes
  }

  dimension: campaign_id {
    type: number
    sql: ${TABLE}.campaign_id ;;
    hidden: yes
  }

  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
    hidden: yes
  }

  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
    hidden: yes
  }

  dimension: cost {
    type: number
    sql: ${TABLE}.cost ;;
    hidden: yes
  }

  dimension: conversions {
    type: number
    sql: ${TABLE}.conversions ;;
    hidden: yes
  }

  measure: total_impressions {
    type: sum
    sql: ${impressions} ;;
  }
  measure: total_clicks {
    type: sum
    sql: ${clicks} ;;
  }
  measure: total_cost {
    type: sum
    sql: ${cost} ;;
  }
  measure: total_conversions {
    type: sum
    sql: ${conversions} ;;
  }
  measure: average_click_rate {
    type: number
    sql: ${total_clicks}*1.0/NULLIF(${total_impressions},0) ;;
  }
}
