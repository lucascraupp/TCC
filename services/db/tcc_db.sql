-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema tcc_db
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema tcc_db
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `tcc_db` ;
USE `tcc_db` ;

-- -----------------------------------------------------
-- Table `tcc_db`.`datetime`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`datetime` (
  `id_datetime` INT NOT NULL AUTO_INCREMENT,
  `timestamp` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id_datetime`),
  UNIQUE INDEX `timestamp_UNIQUE` (`timestamp` ASC) VISIBLE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `tcc_db`.`status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`status` (
  `id_status` INT NOT NULL AUTO_INCREMENT,
  `status` VARCHAR(11) NOT NULL,
  PRIMARY KEY (`id_status`),
  UNIQUE INDEX `status_UNIQUE` (`status` ASC) VISIBLE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `tcc_db`.`sensor`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`sensor` (
  `id_sensor` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NOT NULL,
  `type` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_sensor`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `tcc_db`.`solar_plant`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`solar_plant` (
  `id_solar_plant` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NOT NULL,
  `nickname` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_solar_plant`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE,
  UNIQUE INDEX `nickname_UNIQUE` (`nickname` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 8
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `tcc_db`.`park`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`park` (
  `id_park` INT NOT NULL AUTO_INCREMENT,
  `id_solar_plant` INT NOT NULL,
  `name` VARCHAR(45) NOT NULL,
  `nickname` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_park`),
  INDEX `fk_park_solar_plant1_idx` (`id_solar_plant` ASC) VISIBLE,
  CONSTRAINT `fk_park_solar_plant1`
    FOREIGN KEY (`id_solar_plant`)
    REFERENCES `tcc_db`.`solar_plant` (`id_solar_plant`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
AUTO_INCREMENT = 73
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `tcc_db`.`sensor_per_park`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`sensor_per_park` (
  `id_sensor` INT NOT NULL,
  `id_park` INT NOT NULL,
  PRIMARY KEY (`id_sensor`, `id_park`),
  INDEX `fk_sensor_has_park_park1_idx` (`id_park` ASC) VISIBLE,
  INDEX `fk_sensor_has_park_sensor1_idx` (`id_sensor` ASC) VISIBLE,
  CONSTRAINT `fk_sensor_has_park_sensor1`
    FOREIGN KEY (`id_sensor`)
    REFERENCES `tcc_db`.`sensor` (`id_sensor`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_sensor_has_park_park1`
    FOREIGN KEY (`id_park`)
    REFERENCES `tcc_db`.`park` (`id_park`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `tcc_db`.`data`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`data` (
  `id_datetime` INT NOT NULL,
  `id_park` INT NOT NULL,
  `id_sensor` INT NOT NULL,
  `id_status` INT NOT NULL,
  `value` DECIMAL(11,2) NOT NULL,
  PRIMARY KEY (`id_datetime`, `id_park`, `id_sensor`, `id_status`),
  INDEX `fk_data_date1_idx` (`id_datetime` ASC) VISIBLE,
  INDEX `fk_data_status1_idx` (`id_status` ASC) VISIBLE,
  INDEX `fk_data_sensor_per_park1_idx` (`id_sensor` ASC, `id_park` ASC) VISIBLE,
  CONSTRAINT `fk_data_date1`
    FOREIGN KEY (`id_datetime`)
    REFERENCES `tcc_db`.`datetime` (`id_datetime`),
  CONSTRAINT `fk_data_status1`
    FOREIGN KEY (`id_status`)
    REFERENCES `tcc_db`.`status` (`id_status`),
  CONSTRAINT `fk_data_sensor_per_park1`
    FOREIGN KEY (`id_sensor` , `id_park`)
    REFERENCES `tcc_db`.`sensor_per_park` (`id_sensor` , `id_park`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
