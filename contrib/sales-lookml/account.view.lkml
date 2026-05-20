view: account {
  sql_table_name: @{SALESFORCE_SCHEMA}.account ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
    hidden: yes
  }

  dimension_group: _fivetran_synced {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}._fivetran_synced ;;
    hidden: yes
  }

  dimension: account_number {
    type: string
    sql: ${TABLE}.account_number ;;
  }

  dimension: account_source {
    type: string
    sql: ${TABLE}.account_source ;;
  }

  dimension: annual_revenue {
    type: number
    sql: ${TABLE}.annual_revenue ;;
    hidden: yes
  }

  dimension: billing_city {
    type: string
    sql: ${TABLE}.billing_city ;;
    group_label: "Billing Details"
  }

  dimension: billing_country {
    type: string
    sql: ${TABLE}.billing_country ;;
    group_label: "Billing Details"
  }

  dimension: billing_state {
    type: string
    sql: ${TABLE}.billing_state ;;
    group_label: "Billing Details"
  }

  dimension: industry {
    type: string
    sql: ${TABLE}.industry ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
    label: "Account Name"
  }

  dimension: number_of_employees {
    type: number
    sql: ${TABLE}.number_of_employees ;;
  }

  dimension: owner_id {
    type: string
    sql: ${TABLE}.owner_id ;;
    hidden: yes
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
    label: "Account Type"
  }

  dimension: website {
    type: string
    sql: ${TABLE}.website ;;
  }

  measure: count {
    type: count
    drill_fields: [id, name, industry, type]
  }

  measure: total_revenue {
    type: sum
    sql: ${annual_revenue} ;;
    value_format_name: usd_0
  }

  measure: average_revenue {
    type: average
    sql: ${annual_revenue} ;;
    value_format_name: usd
  }

  measure: total_employees {
    type: sum
    sql: ${number_of_employees} ;;
  }
}
