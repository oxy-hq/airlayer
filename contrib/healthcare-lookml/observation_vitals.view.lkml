view: observation_vitals {
  sql_table_name: healthcare_demo.observation_vitals ;;

  dimension: primary_key {
    hidden: yes
    sql: CONCAT(${observation_id},${type}) ;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: encounter_id {
    type: string
    sql: ${TABLE}.encounter_id ;;
  }

  dimension_group: issued {
    type: time
    timeframes: [
      time,
      raw,
      time_of_day,
      hour,
      date,
      week,
      month,
      year,
      hour_of_day,
      day_of_week
    ]
    sql: ${TABLE}.issued ;;
  }

  dimension: observation_id {
    type: string
    sql: ${TABLE}.observation_id ;;
  }

  dimension: patient_id {
    type: string
    sql: ${TABLE}.patient_id ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: value {
    type: number
    sql: ${TABLE}.value ;;
  }

  dimension: units {
    type: string
    sql: ${TABLE}.units ;;
  }

  measure: count {
    type: count
  }

  measure: average_value {
    type: average
    sql: ${value} ;;
  }

  measure: min_value {
    type: min
    sql: ${value} ;;
  }

  measure: max_value {
    type: max
    sql: ${value} ;;
  }
}
