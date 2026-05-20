explore: opportunity {
  join: account {
    type: left_outer
    sql_on: ${opportunity.account_id} = ${account.id} ;;
    relationship: many_to_one
  }
}
