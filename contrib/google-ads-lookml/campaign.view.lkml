view: campaign {
  sql_table_name: @{GOOGLE_ADS_SCHEMA}.CAMPAIGN_HISTORY ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
    hidden: yes
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
    label: "Campaign"
  }

  dimension: advertising_channel_type {
    type: string
    sql: ${TABLE}.advertising_channel_type ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    label: "Campaign Status"
  }

  dimension: budget_id {
    type: number
    sql: ${TABLE}.budget_id ;;
    hidden: yes
  }

  measure: count {
    type: count_distinct
    sql: ${id} ;;
    drill_fields: [id, name, status]
  }
}
