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
  PRIMARY KEY (`id_solar_plant`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 8
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `tcc_db`.`status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`status` (
  `id_status` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(11) NOT NULL,
  PRIMARY KEY (`id_status`),
  UNIQUE INDEX `status_UNIQUE` (`name` ASC) VISIBLE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `tcc_db`.`solar_plant_has_sensor`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`solar_plant_has_sensor` (
  `id_solar_plant` INT NOT NULL,
  `id_sensor` INT NOT NULL,
  PRIMARY KEY (`id_solar_plant`, `id_sensor`),
  INDEX `fk_solar_plant_has_sensor_sensor1_idx` (`id_sensor` ASC) VISIBLE,
  INDEX `fk_solar_plant_has_sensor_solar_plant1_idx` (`id_solar_plant` ASC) VISIBLE,
  CONSTRAINT `fk_solar_plant_has_sensor_solar_plant1`
    FOREIGN KEY (`id_solar_plant`)
    REFERENCES `tcc_db`.`solar_plant` (`id_solar_plant`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_solar_plant_has_sensor_sensor1`
    FOREIGN KEY (`id_sensor`)
    REFERENCES `tcc_db`.`sensor` (`id_sensor`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `tcc_db`.`classification`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`classification` (
  `id_solar_plant` INT NOT NULL,
  `id_sensor` INT NOT NULL,
  `timestamp` TIMESTAMP NOT NULL,
  `status` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_solar_plant`, `id_sensor`, `timestamp`),
  CONSTRAINT `fk_classification_solar_plant_has_sensor1`
    FOREIGN KEY (`id_solar_plant` , `id_sensor`)
    REFERENCES `tcc_db`.`solar_plant_has_sensor` (`id_solar_plant` , `id_sensor`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `tcc_db`.`solar_plant_data`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`solar_plant_data` (
  `id_solar_plant` INT NOT NULL,
  `id_sensor` INT NOT NULL,
  `id_status` INT NOT NULL,
  `timestamp` TIMESTAMP NOT NULL,
  `value` FLOAT NOT NULL,
  PRIMARY KEY (`id_solar_plant`, `id_sensor`, `id_status`, `timestamp`),
  INDEX `fk_gti_ghi_power_status1_idx` (`id_status` ASC) VISIBLE,
  CONSTRAINT `fk_gti_ghi_power_solar_plant_has_sensor1`
    FOREIGN KEY (`id_solar_plant` , `id_sensor`)
    REFERENCES `tcc_db`.`solar_plant_has_sensor` (`id_solar_plant` , `id_sensor`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_gti_ghi_power_status1`
    FOREIGN KEY (`id_status`)
    REFERENCES `tcc_db`.`status` (`id_status`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `tcc_db`.`external_data`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `tcc_db`.`external_data` (
  `id_solar_plant` INT NOT NULL,
  `id_sensor` INT NOT NULL,
  `timestamp` TIMESTAMP NOT NULL,
  `value` FLOAT NOT NULL,
  PRIMARY KEY (`id_solar_plant`, `id_sensor`, `timestamp`),
  CONSTRAINT `fk_external_data_solar_plant_has_sensor1`
    FOREIGN KEY (`id_solar_plant` , `id_sensor`)
    REFERENCES `tcc_db`.`solar_plant_has_sensor` (`id_solar_plant` , `id_sensor`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
