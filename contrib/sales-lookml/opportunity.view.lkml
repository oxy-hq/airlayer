view: opportunity {
  sql_table_name: @{SALESFORCE_SCHEMA}.opportunity ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
    hidden: yes
  }

  dimension: account_id {
    type: string
    sql: ${TABLE}.account_id ;;
    hidden: yes
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
    label: "Opportunity Name"
  }

  dimension: stage_name {
    type: string
    sql: ${TABLE}.stage_name ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
    label: "Opportunity Type"
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
    hidden: yes
  }

  dimension: is_won {
    type: yesno
    sql: ${TABLE}.is_won ;;
  }

  dimension: is_closed {
    type: yesno
    sql: ${TABLE}.is_closed ;;
  }

  dimension_group: close {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.close_date ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.created_date ;;
  }

  dimension: probability {
    type: number
    sql: ${TABLE}.probability ;;
  }

  measure: count {
    type: count
  }

  measure: total_amount {
    type: sum
    sql: ${amount} ;;
    value_format_name: usd_0
  }

  measure: average_amount {
    type: average
    sql: ${amount} ;;
    value_format_name: usd
  }

  measure: win_count {
    type: count
    filters: {
      field: is_won
      value: "yes"
    }
  }

  measure: win_rate {
    type: number
    sql: ${win_count}*1.0 / NULLIF(${count}, 0) ;;
    value_format_name: percent_2
  }

  measure: total_pipeline {
    type: sum
    sql: ${amount} ;;
    filters: {
      field: is_closed
      value: "no"
    }
  }
}
