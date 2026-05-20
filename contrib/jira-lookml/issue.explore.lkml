explore: issue {
  join: project {
    type: left_outer
    sql_on: ${issue.project_id} = ${project.id} ;;
    relationship: many_to_one
  }
  join: priority {
    type: left_outer
    sql_on: ${issue.priority} = ${priority.id} ;;
    relationship: many_to_one
  }
  join: status {
    type: left_outer
    sql_on: ${issue.status} = ${status.id} ;;
    relationship: many_to_one
  }
  join: status_category {
    type: left_outer
    sql_on: ${status.status_category_id} = ${status_category.id} ;;
    relationship: many_to_one
  }
}
