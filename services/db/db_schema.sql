-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema db_schema
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema db_schema
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `db_schema` ;
USE `db_schema` ;

-- -----------------------------------------------------
-- Table `db_schema`.`solar_usine`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_schema`.`solar_usine` (
  `id_solar_usine` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NOT NULL,
  `nickname` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_solar_usine`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `db_schema`.`park`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_schema`.`park` (
  `id_park` INT NOT NULL AUTO_INCREMENT,
  `id_solar_usine` INT NOT NULL,
  `name` VARCHAR(45) NOT NULL,
  `nickname` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_park`),
  INDEX `fk_park_solar_usine_idx` (`id_solar_usine` ASC) VISIBLE,
  CONSTRAINT `fk_park_solar_usine`
    FOREIGN KEY (`id_solar_usine`)
    REFERENCES `db_schema`.`solar_usine` (`id_solar_usine`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `db_schema`.`datetime`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_schema`.`datetime` (
  `id_datetime` INT NOT NULL AUTO_INCREMENT,
  `timestamp` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id_datetime`),
  UNIQUE INDEX `timestamp_UNIQUE` (`timestamp` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `db_schema`.`status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_schema`.`status` (
  `id_status` INT NOT NULL AUTO_INCREMENT,
  `status` VARCHAR(11) NOT NULL,
  PRIMARY KEY (`id_status`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `db_schema`.`sensor`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_schema`.`sensor` (
  `id_sensor` INT NOT NULL AUTO_INCREMENT,
  `id_park` INT NOT NULL,
  `name` VARCHAR(45) NOT NULL,
  `type` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_sensor`, `id_park`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE,
  INDEX `fk_sensor_park1_idx` (`id_park` ASC) VISIBLE,
  CONSTRAINT `fk_sensor_park1`
    FOREIGN KEY (`id_park`)
    REFERENCES `db_schema`.`park` (`id_park`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `db_schema`.`data`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_schema`.`data` (
  `id_datetime` INT NOT NULL,
  `id_park` INT NOT NULL,
  `id_sensor` INT NOT NULL,
  `id_status` INT NOT NULL,
  `data` DECIMAL(2) NOT NULL,
  PRIMARY KEY (`id_datetime`, `id_park`, `id_sensor`),
  INDEX `fk_data_date1_idx` (`id_datetime` ASC) VISIBLE,
  INDEX `fk_data_sensor1_idx` (`id_sensor` ASC, `id_park` ASC) VISIBLE,
  INDEX `fk_data_status1_idx` (`id_status` ASC) VISIBLE,
  CONSTRAINT `fk_data_date1`
    FOREIGN KEY (`id_datetime`)
    REFERENCES `db_schema`.`datetime` (`id_datetime`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_data_sensor1`
    FOREIGN KEY (`id_sensor` , `id_park`)
    REFERENCES `db_schema`.`sensor` (`id_sensor` , `id_park`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_data_status1`
    FOREIGN KEY (`id_status`)
    REFERENCES `db_schema`.`status` (`id_status`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
