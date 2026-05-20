view: patient {
  sql_table_name: healthcare_demo.patient ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: CONCAT(${TABLE}.first_name, ' ', ${TABLE}.last_name) ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: gender {
    type: string
    sql: ${TABLE}.gender ;;
  }

  dimension_group: birth {
    type: time
    timeframes: [date, month, year]
    sql: ${TABLE}.birth_date ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: zip {
    type: zipcode
    sql: ${TABLE}.zip ;;
  }

  dimension: is_deceased {
    type: yesno
    sql: ${TABLE}.death_date IS NOT NULL ;;
  }

  measure: count {
    type: count
    drill_fields: [id, name, gender, city]
  }

  measure: count_female {
    type: count
    filters: {
      field: gender
      value: "female"
    }
  }

  measure: count_male {
    type: count
    filters: {
      field: gender
      value: "male"
    }
  }
}
