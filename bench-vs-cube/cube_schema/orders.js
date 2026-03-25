cube(`orders`, {
  sql_table: `orders`,

  joins: {
    customers: {
      relationship: `many_to_one`,
      sql: `${CUBE}.customer_id = ${customers}.id`,
    },
    line_items: {
      relationship: `one_to_many`,
      sql: `${CUBE}.id = ${line_items}.order_id`,
    },
  },

  dimensions: {
    order_id: {
      sql: `id`,
      type: `number`,
      primary_key: true,
    },
    customer_id: {
      sql: `customer_id`,
      type: `number`,
    },
    status: {
      sql: `status`,
      type: `string`,
    },
    amount: {
      sql: `amount`,
      type: `number`,
    },
    created_at: {
      sql: `created_at`,
      type: `time`,
    },
    channel: {
      sql: `channel`,
      type: `string`,
    },
    country: {
      sql: `country`,
      type: `string`,
    },
    discount_pct: {
      sql: `discount_pct`,
      type: `number`,
    },
  },

  measures: {
    count: {
      type: `count`,
    },
    total_revenue: {
      sql: `amount`,
      type: `sum`,
    },
    avg_order_value: {
      sql: `amount`,
      type: `avg`,
    },
    unique_customers: {
      sql: `customer_id`,
      type: `count_distinct`,
    },
    completed_count: {
      type: `count`,
      filters: [{ sql: `${CUBE}.status = 'completed'` }],
    },
  },
});
