CREATE TABLE `file_data_source` (
  `file_data_src_id` bigint NOT NULL AUTO_INCREMENT,
  `data_provider_conn_dtl_id` bigint NOT NULL,
  `file_data_src_file_name_pattern` varchar(100) DEFAULT NULL,
  `file_data_src_file_type` varchar(100) DEFAULT NULL,
  `file_data_src_tgt_loc` json DEFAULT NULL,
  `file_data_src_src_loc` json DEFAULT NULL,
  `inserted_by` varchar(100) NOT NULL,
  `inserted_on` timestamp NULL DEFAULT NULL,
  `updated_by` varchar(100) DEFAULT NULL,
  `updated_on` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`file_data_src_id`),
  KEY `data_provider_conn_dtl_id` (`data_provider_conn_dtl_id`),
  CONSTRAINT `file_data_source_ibfk_1` FOREIGN KEY (`data_provider_conn_dtl_id`) REFERENCES `data_provider_connection_detail` (`data_provider_conn_dtl_id`)
) ENGINE=InnoDB AUTO_INCREMENT=64 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci