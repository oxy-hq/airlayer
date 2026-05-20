view: project {
  sql_table_name: @{SCHEMA_NAME}.project ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: key {
    type: string
    sql: ${TABLE}.key ;;
  }

  measure: count {
    type: count
  }
}
