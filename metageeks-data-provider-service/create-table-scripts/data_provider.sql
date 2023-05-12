CREATE TABLE `data_provider` (
  `data_provider_id` bigint NOT NULL AUTO_INCREMENT,
  `data_provider_name` varchar(100) NOT NULL,
  `data_provider_type` varchar(100) NOT NULL,
  `inserted_by` varchar(100) NOT NULL,
  `inserted_on` timestamp NOT NULL,
  `updated_by` varchar(100) DEFAULT NULL,
  `updated_on` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`data_provider_id`)
) ENGINE=InnoDB AUTO_INCREMENT=45 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
