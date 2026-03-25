cube(`line_items`, {
  sql_table: `line_items`,

  joins: {
    products: {
      relationship: `many_to_one`,
      sql: `${CUBE}.product_id = ${products}.id`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primary_key: true,
    },
    order_id: {
      sql: `order_id`,
      type: `number`,
    },
    product_id: {
      sql: `product_id`,
      type: `number`,
    },
    quantity: {
      sql: `quantity`,
      type: `number`,
    },
    unit_price: {
      sql: `unit_price`,
      type: `number`,
    },
    line_total: {
      sql: `${CUBE}.quantity * ${CUBE}.unit_price`,
      type: `number`,
    },
  },

  measures: {
    total_quantity: {
      sql: `quantity`,
      type: `sum`,
    },
    total_line_value: {
      sql: `${CUBE}.quantity * ${CUBE}.unit_price`,
      type: `sum`,
    },
    avg_unit_price: {
      sql: `unit_price`,
      type: `avg`,
    },
    item_count: {
      type: `count`,
    },
  },
});
