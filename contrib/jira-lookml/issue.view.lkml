view: issue {
  sql_table_name: @{SCHEMA_NAME}.issue ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension_group: _fivetran_synced {
    type: time
    hidden: yes
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}._FIVETRAN_SYNCED ;;
  }

  dimension: key {
    type: string
    sql: ${TABLE}.key ;;
    link: {
      url: "http://company.atlassian.net/browse/{{ value }}"
      label: "View in Jira"
    }
  }

  dimension: epic_link {
    type: string
    sql: ${TABLE}.epic_link ;;
    description: "Epic ID Link"
    hidden: yes
  }

  dimension: priority {
    type: number
    hidden: yes
    sql: ${TABLE}.priority ;;
  }

  dimension: resolution {
    group_label: "Resolution"
    hidden: yes
    type: number
    sql: ${TABLE}.resolution ;;
  }

  dimension: status {
    #hidden: yes
    type: number
    sql: ${TABLE}.status ;;
  }

  dimension: parent_id {
    type: number
    sql: ${TABLE}.parent_id ;;
  }

  dimension: needs_triage {
    type: yesno
    description: "Issues with no priority are labeled as needing triage."
    sql: CASE WHEN ${priority.name} IS NULL THEN true ELSE false END ;;
  }

  measure: count {
    type: count
  }

  measure: number_of_open_issues {
    type: count
    filters: {
      field: status_category.name
      value: "-Closed"
    }
  }

  measure: number_of_closed_issues {
    type: count
    filters: {
      field: status_category.name
      value: "Closed"
    }
  }
}
