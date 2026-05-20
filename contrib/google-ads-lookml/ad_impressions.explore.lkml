explore: ad_impressions {
  from: ad_impressions
  view_name: fact
  join: campaign {
    type: left_outer
    sql_on: ${fact.campaign_id} = ${campaign.id} ;;
    relationship: many_to_one
  }
}
