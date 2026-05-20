explore: encounter {
  join: patient {
    type: left_outer
    sql_on: ${encounter.patient_id} = ${patient.id} ;;
    relationship: many_to_one
  }
  join: observation_vitals {
    type: left_outer
    sql_on: ${encounter.id} = ${observation_vitals.encounter_id} ;;
    relationship: one_to_many
  }
}
