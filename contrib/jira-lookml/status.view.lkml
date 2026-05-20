view: status {
  sql_table_name: @{SCHEMA_NAME}.status ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: description {
    type: string
    sql: ${TABLE}.description ;;
  }

  dimension: status_category_id {
    type: number
    sql: ${TABLE}.status_category_id ;;
  }

  measure: count {
    type: count
  }
}
