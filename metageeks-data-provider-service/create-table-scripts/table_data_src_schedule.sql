CREATE TABLE `table_data_src_schedule` (
  `table_data_src_schedule_id` bigint NOT NULL AUTO_INCREMENT,
  `table_data_src_id` bigint NOT NULL,
  `table_data_src_schedule` json DEFAULT NULL,
  `inserted_by` varchar(30) DEFAULT NULL,
  `inserted_on` timestamp NULL DEFAULT NULL,
  `updated_by` varchar(30) DEFAULT NULL,
  `updated_on` timestamp NULL DEFAULT NULL,
  `dags_created` varchar(50) DEFAULT NULL,
  `table_data_schedule_valid_from` timestamp NULL DEFAULT NULL,
  `table_data_schedule_valid_to` timestamp NULL DEFAULT NULL,
  `table_data_schedule_frequency` varchar(100) DEFAULT NULL,
  `table_data_schedule_interval` int DEFAULT NULL,
  PRIMARY KEY (`table_data_src_schedule_id`),
  KEY `table_data_src_id` (`table_data_src_id`),
  CONSTRAINT `table_data_src_schedule_ibfk_1` FOREIGN KEY (`table_data_src_id`) REFERENCES `table_data_src` (`table_data_src_id`)
) ENGINE=InnoDB AUTO_INCREMENT=39 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci