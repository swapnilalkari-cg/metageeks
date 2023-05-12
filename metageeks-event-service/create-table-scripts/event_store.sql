CREATE TABLE `event_store` (
  `event_id` bigint NOT NULL AUTO_INCREMENT,
  `event_data` json DEFAULT NULL,
  `event_sequence` varchar(255) DEFAULT NULL,
  `event_source` varchar(255) DEFAULT NULL,
  `event_time` datetime DEFAULT NULL,
  `event_type` varchar(255) NOT NULL,
  PRIMARY KEY (`event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=728 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
