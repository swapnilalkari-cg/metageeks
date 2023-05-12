CREATE TABLE `table_data_src` (
  `table_data_src_id` bigint NOT NULL AUTO_INCREMENT,
  `data_provider_conn_dtl_id` bigint NOT NULL,
  `table_owner` varchar(100) DEFAULT NULL,
  `table_name` varchar(100) DEFAULT NULL,
  `is_incremental_extract` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `insert_ts_col` varchar(100) DEFAULT NULL,
  `update_ts_col` varchar(100) DEFAULT NULL,
  `inserted_by` varchar(100) NOT NULL,
  `inserted_on` timestamp NOT NULL,
  `updated_by` varchar(100) DEFAULT NULL,
  `updated_on` timestamp NULL DEFAULT NULL,
  `table_data_src_tgt_loc` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`table_data_src_id`),
  KEY `data_provider_conn_dtl_id` (`data_provider_conn_dtl_id`),
  CONSTRAINT `table_data_src_ibfk_1` FOREIGN KEY (`data_provider_conn_dtl_id`) REFERENCES `data_provider_connection_detail` (`data_provider_conn_dtl_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci