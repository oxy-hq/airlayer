view: orders {
  sql_table_name: public.orders ;;
  description: "E-commerce order data"

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
  }

  dimension: is_completed {
    type: yesno
    sql: ${TABLE}.status = 'completed' ;;
  }

  measure: count {
    type: count
    drill_fields: [id, status, amount]
  }

  measure: total_revenue {
    type: sum
    sql: ${TABLE}.amount ;;
    description: "Total order revenue"
  }

  measure: avg_order_value {
    type: average
    sql: ${TABLE}.amount ;;
  }

  measure: unique_customers {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
  }

  measure: max_order_amount {
    type: max
    sql: ${TABLE}.amount ;;
  }

  measure: min_order_amount {
    type: min
    sql: ${TABLE}.amount ;;
  }
}

view: customers {
  sql_table_name: public.customers ;;
  description: "Customer profiles"

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
  }

  dimension_group: created {
    type: time
    timeframes: [date, month, year]
    sql: ${TABLE}.created_at ;;
  }

  measure: count {
    type: count
  }
}

view: products {
  sql_table_name: public.products ;;
  description: "Product catalog"

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
  }

  measure: count {
    type: count
  }

  measure: avg_price {
    type: average
    sql: ${TABLE}.price ;;
  }
}

explore: orders {
  description: "Orders with customer and product details"

  join: customers {
    sql_on: ${orders.user_id} = ${customers.id} ;;
    relationship: many_to_one
  }
}
