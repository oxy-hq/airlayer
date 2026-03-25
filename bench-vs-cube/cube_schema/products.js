cube(`products`, {
  sql_table: `products`,

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
    category: {
      sql: `category`,
      type: `string`,
    },
    brand: {
      sql: `brand`,
      type: `string`,
    },
    sku: {
      sql: `sku`,
      type: `string`,
    },
    list_price: {
      sql: `list_price`,
      type: `number`,
    },
  },

  measures: {
    count: {
      type: `count`,
    },
    avg_list_price: {
      sql: `list_price`,
      type: `avg`,
    },
    unique_brands: {
      sql: `brand`,
      type: `count_distinct`,
    },
  },
});
