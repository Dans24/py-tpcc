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
  PRIMARY KEY ("d_id", "d_w_id"),
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
  "c_discount" numeric(4,4),
  "c_data" varchar(500),
  PRIMARY KEY ("c_id", "c_d_id", "c_w_id"),
  FOREIGN KEY ("c_d_id", "c_w_id") REFERENCES "district" ("d_id", "d_w_id")
);

CREATE TABLE "history" (
  "h_c_id" int,
  "h_c_d_id" int,
  "h_c_w_id" int,
  "h_d_id" int,
  "h_w_id" int,
  "h_date" timestamp,
  "h_amount" numeric(6, 2),
  "h_data" varchar(24),
  PRIMARY KEY ("h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date"),
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
  "o_entry_d" timestamp,
  "o_d_id" int,
  "o_w_id" int,
  "o_c_id" int,
  PRIMARY KEY ("o_entry_d", "o_d_id", "o_w_id"),
  FOREIGN KEY ("o_d_id", "o_w_id", "o_c_id") REFERENCES "customer" ("c_d_id", "c_w_id", "c_id")
);

CREATE TABLE "order_line" (
  "ol_entry_d" timestamp,
  "ol_d_id" int,
  "ol_w_id" int,
  "ol_number" int,
  "ol_i_id" int,
  "ol_supply_w_id" int,
  "ol_quantity" int,
  "ol_dist_info" char(24),
  PRIMARY KEY ("ol_entry_d", "ol_d_id", "ol_w_id", "ol_number"),
  FOREIGN KEY ("ol_entry_d", "ol_d_id", "ol_w_id") REFERENCES "orders" ("o_entry_d", "o_d_id", "o_w_id"),
  FOREIGN KEY ("ol_i_id") REFERENCES "item" ("i_id"),
  FOREIGN KEY ("ol_supply_w_id") REFERENCES "warehouse" ("w_id")
);

CREATE TABLE "delivery" (
  "dl_delivery_d" timestamp PRIMARY KEY,
  "dl_w_id" int,
  "dl_carrier_id" int,
  FOREIGN KEY ("dl_w_id") REFERENCES "warehouse" ("w_id")
);

--CREATE INDEX IDX_ORDER_LINE_3COL ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);
--CREATE INDEX IDX_ORDER_LINE_2COL ON ORDER_LINE (OL_W_ID,OL_D_ID);
CREATE INDEX IDX_ORDER_LINE_TREE ON "order_line" ("ol_d_id","ol_w_id","ol_entry_d");
