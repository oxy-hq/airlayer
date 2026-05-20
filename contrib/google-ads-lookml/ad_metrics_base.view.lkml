view: ad_metrics_base {
  extension: required

  dimension: clicks {
    hidden: yes
    type: number
    sql: ${TABLE}.clicks ;;
  }
  dimension: conversions {
    hidden: yes
    type: number
    sql: ${TABLE}.conversions ;;
  }
  dimension: conversionvalue {
    hidden: yes
    type: number
    sql: ${TABLE}.conversionvalue ;;
  }
  dimension: cost {
    hidden: yes
    type: number
    sql: ${TABLE}.cost ;;
  }
  dimension: impressions {
    hidden: yes
    type: number
    sql: ${TABLE}.impressions ;;
  }
  dimension: click_rate {
    hidden: yes
    label: "Click Through Rate"
    description: "Percent of people that click on an ad."
    type: number
    sql: ${clicks}*1.0/nullif(${impressions},0) ;;
    value_format_name: percent_2
  }

  measure: total_clicks {
    label: "Clicks"
    description: "Total ad clicks."
    type: sum
    sql: ${clicks} ;;
    value_format_name: decimal_0
    drill_fields: [fact.date_date, campaign.name, total_clicks]
  }
  measure: total_conversions {
    label: "Conversions"
    description: "Total conversions."
    type: sum
    sql: ${conversions} ;;
    value_format_name: decimal_0
  }
  measure: total_cost {
    label: "Cost"
    description: "Total cost."
    type: sum
    sql: ${cost} ;;
    value_format_name: usd_0
  }
  measure: total_impressions {
    label: "Impressions"
    description: "Total ad impressions."
    type:  sum
    sql:  ${impressions} ;;
    value_format_name: decimal_0
  }
  measure: average_click_rate {
    label: "Click Through Rate"
    description: "Percent of people that click on an ad."
    type: number
    sql: ${total_clicks}*1.0/nullif(${total_impressions},0) ;;
    value_format_name: percent_2
  }
  measure: cumulative_spend {
    type: running_total
    sql: ${total_cost} ;;
    value_format_name: usd_0
    direction: "column"
  }

  set: ad_metrics_set {
    fields: [
      cost,
      impressions,
      clicks,
      conversions,
      total_cost,
      total_impressions,
      total_clicks,
      total_conversions,
      average_click_rate,
      cumulative_spend
    ]
  }
}
