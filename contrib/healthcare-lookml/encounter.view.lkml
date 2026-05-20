view: encounter {
  sql_table_name: healthcare_demo.encounter ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: patient_id {
    type: string
    sql: ${TABLE}.patient_id ;;
  }

  dimension: encounter_class {
    type: string
    sql: ${TABLE}.encounter_class ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: description {
    type: string
    sql: ${TABLE}.description ;;
  }

  dimension_group: start {
    type: time
    timeframes: [time, raw, date, week, month, quarter, year, hour_of_day, day_of_week]
    sql: ${TABLE}.start_time ;;
  }

  dimension_group: end {
    type: time
    timeframes: [time, raw, date, week, month, quarter, year]
    sql: ${TABLE}.end_time ;;
  }

  dimension_group: length_of_stay {
    type: duration
    intervals: [day, hour, minute]
    sql_start: ${start_raw} ;;
    sql_end: ${end_raw} ;;
  }

  dimension: organization_id {
    type: string
    sql: ${TABLE}.organization_id ;;
  }

  dimension: total_claim_cost {
    type: number
    sql: ${TABLE}.total_claim_cost ;;
  }

  measure: count {
    type: count
  }

  measure: total_cost {
    type: sum
    sql: ${total_claim_cost} ;;
  }

  measure: average_cost {
    type: average
    sql: ${total_claim_cost} ;;
  }

  measure: count_distinct_patients {
    type: count_distinct
    sql: ${patient_id} ;;
  }
}
