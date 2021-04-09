CREATE TABLE "warehouse" (
  "w_id" int PRIMARY KEY,
  "w_name" varchar(10),
  "w_street_1" varchar(20),
  "w_street_2" varchar(20),
  "w_city" varchar(20),
  "w_state" char(2),
  "w_zip" char(9),
  "w_tax" numeric(4,4)
);

CREATE TABLE "district" (
  "d_id" int,
  "d_w_id" int,
  "d_name" varchar(10),
  "d_street_1" varchar(20),
  "d_street_2" varchar(20),
  "d_city" varchar(20),
  "d_state" char(2),
  "d_zip" char(9),
  "d_tax" numeric(4,4),
  PRIMARY KEY ("d_w_id", "d_id"),
  FOREIGN KEY ("d_w_id") REFERENCES "warehouse" ("w_id")
);

CREATE TABLE "customer" (
  "c_id" int,
  "c_d_id" int,
  "c_w_id" int,
  "c_first" varchar(16),
  "c_middle" char(2),
  "c_last" varchar(16),
  "c_street_1" varchar(20),
  "c_street_2" varchar(20),
  "c_city" varchar(20),
  "c_state" char(2),
  "c_zip" char(9),
  "c_phone" char(16),
  "c_since" timestamp,
  "c_credit" char(2),
  "c_credit_lim" numeric(12, 2),
  "c_discount" numeric(4,4),
  "c_data" varchar(500),
  PRIMARY KEY ("c_w_id", "c_d_id", "c_id"),
  FOREIGN KEY ("c_d_id", "c_w_id") REFERENCES "district" ("d_id", "d_w_id")
);

CREATE INDEX "customer_last" ON "customer" ("c_w_id", "c_d_id", "c_last");

CREATE TABLE "history" (
  "h_c_id" int,
  "h_c_d_id" int,
  "h_c_w_id" int,
  "h_d_id" int,
  "h_w_id" int,
  "h_date" timestamp,
  "h_amount" numeric(6, 2),
  "h_data" varchar(24),
  PRIMARY KEY ("h_c_w_id", "h_c_d_id", "h_c_id", "h_w_id", "h_d_id", "h_date"),
  FOREIGN KEY ("h_c_id", "h_c_d_id", "h_c_w_id") REFERENCES "customer" ("c_id", "c_d_id", "c_w_id"),
  FOREIGN KEY ("h_d_id", "h_w_id") REFERENCES "district" ("d_id", "d_w_id")
);

CREATE TABLE "item" (
  "i_id" int PRIMARY KEY,
  "i_im_id" int,
  "i_name" varchar(24),
  "i_price" numeric(5,2),
  "i_data" varchar(50)
);

CREATE TABLE "orders" (
  "o_id" int,
  "o_d_id" int,
  "o_w_id" int,
  "o_c_id" int,
  "o_ol_cnt" int,
  "o_all_local" boolean,
  "o_entry_d" timestamp,
  PRIMARY KEY ("o_w_id", "o_d_id", "o_id"),
  FOREIGN KEY ("o_d_id", "o_w_id", "o_c_id") REFERENCES "customer" ("c_d_id", "c_w_id", "c_id")
);

-- Optimize order_status.getCustomerByCustomerId/LastName
CREATE INDEX "orders_customer_fk" ON "orders" ("o_d_id", "o_w_id", "o_c_id");

CREATE TABLE "order_line" (
  "ol_o_id" int,
  "ol_d_id" int,
  "ol_w_id" int,
  "ol_number" int,
  "ol_i_id" int,
  "ol_supply_w_id" int,
  "ol_quantity" int,
  "ol_amount" numeric(6,2),
  "ol_dist_info" char(24),
  PRIMARY KEY ("ol_w_id", "ol_d_id", "ol_o_id", "ol_number"),
  FOREIGN KEY ("ol_o_id", "ol_d_id", "ol_w_id") REFERENCES "orders" ("o_id", "o_d_id", "o_w_id"),
  FOREIGN KEY ("ol_i_id") REFERENCES "item" ("i_id"),
  FOREIGN KEY ("ol_supply_w_id") REFERENCES "warehouse" ("w_id")
);

-- Optimize new_order.getStockInfo
CREATE INDEX "order_line_stock_fk" ON "order_line" ("ol_supply_w_id", "ol_i_id");

CREATE TABLE "delivery" (
  "dl_delivery_d" timestamp,
  "dl_w_id" int,
  "dl_carrier_id" int,
  PRIMARY KEY ("dl_w_id", "dl_delivery_d"),
  FOREIGN KEY ("dl_w_id") REFERENCES "warehouse" ("w_id")
);

CREATE TABLE "delivery_orders" (
  "dlo_delivery_d" timestamp,
  "dlo_w_id" int,
  "dlo_o_id" int,
  "dlo_d_id" int,
  PRIMARY KEY ("dlo_w_id", "dlo_delivery_d", "dlo_o_id", "dlo_d_id"),
  FOREIGN KEY ("dlo_delivery_d", "dlo_w_id") REFERENCES "delivery" ("dl_delivery_d", "dl_w_id"),
  FOREIGN KEY ("dlo_o_id", "dlo_d_id", "dlo_w_id") REFERENCES "orders" ("o_id", "o_d_id", "o_w_id")
);

CREATE INDEX "delivery_orders_orders_fk" ON "delivery_orders" ("dlo_w_id", "dlo_d_id", "dlo_o_id");

CREATE TABLE "stock" (
  "s_i_id" int,
  "s_w_id" int,
  "s_dist_01" char(24),
  "s_dist_02" char(24),
  "s_dist_03" char(24),
  "s_dist_04" char(24), 
  "s_dist_05" char(24),
  "s_dist_06" char(24),
  "s_dist_07" char(24),
  "s_dist_08" char(24),
  "s_dist_09" char(24),
  "s_dist_10" char(24),
  "s_data" varchar(50),
  PRIMARY KEY ("s_w_id", "s_i_id"),
  FOREIGN KEY ("s_i_id") REFERENCES "item" ("i_id"),
  FOREIGN KEY ("s_w_id") REFERENCES "warehouse" ("w_id")
);

--CREATE INDEX IDX_ORDER_LINE_3COL ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);
--CREATE INDEX IDX_ORDER_LINE_2COL ON ORDER_LINE (OL_W_ID,OL_D_ID);
--CREATE INDEX IDX_ORDER_LINE_TREE ON "order_line" ("ol_d_id","ol_w_id","ol_o_id");

CREATE TABLE "stock_history" (
  "sh_s_i_id" int,
  "sh_s_w_id" int,
  "sh_date" timestamp,
  "sh_quantity" int,
  PRIMARY KEY ("sh_s_w_id", "sh_s_i_id", "sh_date"),
  FOREIGN KEY ("sh_s_w_id", "sh_s_i_id") REFERENCES "stock" ("s_w_id", "s_i_id")
);

CREATE INDEX "stock_history_date_desc_idx" ON "stock_history" ("sh_s_w_id", "sh_s_i_id", "sh_date" DESC);

CREATE TABLE "customer_history" (
  "ch_c_id" int,
  "ch_c_d_id" int,
  "ch_c_w_id" int,
  "ch_date" timestamp,
  "ch_data" varchar(500),
  PRIMARY KEY ("ch_c_w_id", "ch_c_d_id", "ch_c_id", "ch_date"),
  FOREIGN KEY ("ch_c_w_id", "ch_c_d_id", "ch_c_id") REFERENCES "customer" ("c_w_id", "c_d_id", "c_id")
);

CREATE INDEX "customer_history_date_desc_idx" ON "customer_history" ("ch_c_w_id", "ch_c_d_id", "ch_c_id", "ch_date" DESC);

CREATE VIEW "orders_view" AS (
  SELECT
    o_id,
    o_d_id,
    o_w_id,
    o_c_id,
    o_entry_d,
    dl_carrier_id AS o_carrier_id,
    o_ol_cnt,
    o_all_local
  FROM orders
  LEFT JOIN delivery_orders
  ON dlo_o_id = o_id AND dlo_d_id = o_d_id AND dlo_w_id = o_w_id
  LEFT JOIN delivery
  ON dlo_delivery_d = dl_delivery_d AND dlo_w_id = dl_w_id
);

CREATE VIEW "new_order_view" AS (
  SELECT
    o_id AS no_o_id,
    o_d_id AS no_d_id,
    o_w_id AS no_w_id
  FROM orders
  LEFT JOIN delivery_orders
  ON dlo_o_id = o_id AND dlo_d_id = o_d_id AND dlo_w_id = o_w_id
  WHERE dlo_o_id IS NULL
);

CREATE VIEW "order_line_view" AS (
  SELECT
    ol_o_id,
    ol_d_id,
    ol_w_id,
    ol_number,
    ol_i_id,
    ol_supply_w_id,
    dlo_delivery_d AS ol_delivery_d,
    ol_quantity,
    ol_amount,
    ol_dist_info
  FROM order_line
  LEFT JOIN orders
  ON ol_i_id = o_id AND ol_d_id = o_d_id AND ol_w_id = o_w_id
  LEFT JOIN delivery_orders
  ON dlo_o_id = o_id AND dlo_d_id = o_d_id AND dlo_w_id = o_w_id
);

CREATE VIEW "stock_view" AS (
  WITH order_line_stats AS (
    SELECT
      ol_i_id AS s_i_id,
      ol_supply_w_id AS s_w_id,
      SUM(ol_amount) AS s_ytd,
      COUNT(*) AS s_order_cnt,
      COUNT(NULLIF(ol_supply_w_id, ol_w_id)) AS s_remote_cnt
    FROM order_line
    GROUP BY ol_i_id, ol_supply_w_id
  )
  SELECT
    DISTINCT ON (s_w_id, s_i_id)
    s_w_id,
    s_i_id,
    sh_quantity as s_quantity,
    s_dist_01,
    s_dist_02,
    s_dist_03,
    s_dist_04,
    s_dist_05,
    s_dist_06,
    s_dist_07,
    s_dist_08,
    s_dist_09,
    s_dist_10,
    s_ytd,
    s_order_cnt,
    s_remote_cnt,
    s_data
  FROM stock
  INNER JOIN stock_history
  ON s_w_id = sh_s_w_id AND s_i_id = sh_s_i_id
  LEFT JOIN order_line_stats USING (s_i_id, s_w_id)
  ORDER BY s_w_id, s_i_id, sh_date DESC
);

CREATE VIEW "customer_view" AS (
  WITH order_line_stats AS (
    SELECT
      o_c_id AS c_id,
      ol_d_id AS c_d_id,
      ol_w_id AS c_w_id,
      SUM(ol_amount) AS ol_total
    FROM order_line
    INNER JOIN orders_view
    ON o_id = ol_o_id AND o_d_id = ol_d_id AND o_w_id = ol_w_id
    GROUP BY o_c_id, ol_d_id, ol_w_id
  ),
  history_stats AS (
    SELECT
      h_c_d_id AS c_d_id,
      h_c_w_id AS c_w_id,
      h_c_id AS c_id,
      SUM(h_amount) AS c_ytd_payment,
      COUNT(*) AS c_payment_cnt
    FROM history
    GROUP BY h_c_d_id, h_c_w_id, h_c_id
  ),
  delivery_stats AS (
    SELECT
      o_c_id AS c_id,
      o_d_id AS c_d_id,
      o_w_id AS c_w_id,
      COUNT(dlo_delivery_d) AS c_delivery_cnt
    FROM orders
    LEFT JOIN delivery_orders
    ON dlo_o_id = o_id AND dlo_d_id = o_d_id AND dlo_w_id = o_w_id
    GROUP BY o_c_id, o_d_id, o_w_id
  )
  SELECT
    DISTINCT ON (c_id, c_d_id, c_w_id)
    c_id,
    c_d_id,
    c_w_id,
    c_first,
    c_middle,
    c_last,
    c_street_1,
    c_street_2,
    c_city,
    c_state,
    c_zip,
    c_phone,
    c_since,
    c_credit,
    c_credit_lim,
    c_discount,
    ol_total - c_ytd_payment AS c_balance,
    c_ytd_payment,
    c_payment_cnt,
    c_delivery_cnt,
    ch_data AS c_data
  FROM customer
  INNER JOIN customer_history
  ON c_id = ch_c_id AND c_d_id = ch_c_d_id AND c_w_id = ch_c_w_id
  LEFT JOIN order_line_stats USING(c_id, c_d_id, c_w_id)
  LEFT JOIN history_stats USING(c_id, c_d_id, c_w_id)
  LEFT JOIN delivery_stats USING(c_id, c_d_id, c_w_id)
  ORDER BY c_w_id, c_d_id, c_id, ch_date DESC
);

CREATE VIEW "warehouse_view" AS (
  WITH history_stats AS (
    SELECT
      h_w_id AS w_id,
      SUM(h_amount) as w_ytd
    FROM history
    GROUP BY h_w_id
  )
  SELECT
    w_id,
    w_name,
    w_street_1,
    w_street_2,
    w_city,
    w_state,
    w_zip,
    w_tax,
    w_ytd
  FROM warehouse
  LEFT JOIN history_stats USING(w_id)
);

CREATE VIEW "district_view" AS (
  WITH history_stats AS (
    SELECT
      h_d_id AS d_id,
      h_w_id AS d_w_id,
      SUM(h_amount) as d_ytd
    FROM history
    GROUP BY h_w_id, h_d_id
  ),
  orders_stats AS (
    SELECT
      o_d_id AS d_id,
      o_w_id AS d_w_id,
      MAX(o_id) as d_last_o_id
    FROM orders
    GROUP BY o_d_id, o_w_id
  )
  SELECT
    d_id,
    d_w_id,
    d_name,
    d_street_1,
    d_street_2,
    d_city,
    d_state,
    d_zip,
    d_tax,
    d_ytd,
    d_last_o_id + 1 AS d_next_o_id
  FROM district
  LEFT JOIN history_stats USING(d_id, d_w_id)
  LEFT JOIN orders_stats USING(d_id, d_w_id)
);