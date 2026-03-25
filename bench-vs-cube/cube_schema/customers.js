cube(`customers`, {
  sql_table: `customers`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primary_key: true,
    },
    name: {
      sql: `name`,
      type: `string`,
    },
    email: {
      sql: `email`,
      type: `string`,
    },
    city: {
      sql: `city`,
      type: `string`,
    },
    country: {
      sql: `country`,
      type: `string`,
    },
    signup_date: {
      sql: `signup_date`,
      type: `time`,
    },
    tier: {
      sql: `tier`,
      type: `string`,
    },
  },

  measures: {
    count: {
      type: `count`,
    },
    unique_countries: {
      sql: `country`,
      type: `count_distinct`,
    },
  },
});
