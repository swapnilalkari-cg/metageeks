CREATE TABLE `data_provider_connection_detail` (
  `data_provider_conn_dtl_id` bigint NOT NULL AUTO_INCREMENT,
  `data_provider_id` bigint NOT NULL,
  `data_provider_conn_name` varchar(100) DEFAULT NULL,
  `data_provider_conn_type` varchar(100) DEFAULT NULL,
  `data_provider_connection_details` json DEFAULT NULL,
  `inserted_by` varchar(100) NOT NULL,
  `inserted_on` timestamp NOT NULL,
  `updated_by` varchar(100) DEFAULT NULL,
  `updated_on` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`data_provider_conn_dtl_id`),
  KEY `data_provider_id` (`data_provider_id`),
  CONSTRAINT `data_provider_connection_detail_ibfk_1` FOREIGN KEY (`data_provider_id`) REFERENCES `data_provider` (`data_provider_id`)
) ENGINE=InnoDB AUTO_INCREMENT=45 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci