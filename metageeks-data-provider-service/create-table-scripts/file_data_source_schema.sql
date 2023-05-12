CREATE TABLE `file_data_source_schema` (
  `file_data_schema_id` bigint NOT NULL AUTO_INCREMENT,
  `file_data_src_id` bigint NOT NULL,
  `file_data_schema_seq_numb` int NOT NULL,
  `schema_name` varchar(100) DEFAULT NULL,
  `file_data_src_schema` json NOT NULL,
  PRIMARY KEY (`file_data_schema_id`),
  KEY `file_data_src_id` (`file_data_src_id`),
  CONSTRAINT `file_data_source_schema_ibfk_1` FOREIGN KEY (`file_data_src_id`) REFERENCES `file_data_source` (`file_data_src_id`)
) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci