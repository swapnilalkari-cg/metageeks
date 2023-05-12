CREATE TABLE `file_data_source_schedule` (
  `file_data_schedule_id` bigint NOT NULL AUTO_INCREMENT,
  `file_data_src_id` bigint NOT NULL,
  `file_data_schedule_frequency` varchar(100) NOT NULL,
  `file_data_schedule_interval` int NOT NULL,
  `file_data_schedule_schedule` json NOT NULL,
  `file_data_schedule_valid_from` timestamp NOT NULL,
  `file_data_schedule_valid_to` timestamp NULL DEFAULT NULL,
  `inserted_by` varchar(100) NOT NULL,
  `inserted_on` timestamp NOT NULL,
  `updated_by` varchar(100) DEFAULT NULL,
  `updated_on` timestamp NULL DEFAULT NULL,
  `dags_created` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`file_data_schedule_id`),
  KEY `file_data_src_id` (`file_data_src_id`),
  CONSTRAINT `file_data_source_schedule_ibfk_1` FOREIGN KEY (`file_data_src_id`) REFERENCES `file_data_source` (`file_data_src_id`)
) ENGINE=InnoDB AUTO_INCREMENT=61 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci