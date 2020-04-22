DROP SCHEMA `proj`;
CREATE SCHEMA `proj`;

CREATE TABLE `proj`.`customer` (
  `C_ID` INT NOT NULL,
  `C_Name` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`C_ID`),
  UNIQUE INDEX `C_ID_UNIQUE` (`C_ID` ASC) VISIBLE)
COMMENT = 'Contains cutomer information';

CREATE TABLE `proj`.`auction` (
  `Auc_ID` INT NOT NULL,
  `Auc_Name` VARCHAR(30) NOT NULL,
  `Period` INT NOT NULL,
  PRIMARY KEY (`Auc_ID`),
  UNIQUE INDEX `Auc_ID_UNIQUE` (`Auc_ID` ASC) VISIBLE)
COMMENT = 'Contians acutions information';

CREATE TABLE `proj`.`location` (
  `L_ID` INT NOT NULL,
  `L_Name` VARCHAR(45) NOT NULL,
  `L_Type` VARCHAR(30) NOT NULL,
  PRIMARY KEY (`L_ID`),
  UNIQUE INDEX `L_ID_UNIQUE` (`L_ID` ASC) VISIBLE)
COMMENT = 'Contains location information';

CREATE TABLE `proj`.`auction_results` (
  `Auc_Res_ID` INT NOT NULL,
  `Auc_ID` INT NOT NULL,
  `C_ID` INT NOT NULL,
  `Sou_ID` INT NOT NULL,
  `Sin_ID` INT NOT NULL,
  `Buy_Sell` VARCHAR(45) NOT NULL,
  `Class_Type` VARCHAR(45) NOT NULL,
  `Award_FTR_MW` DECIMAL(6,2) NOT NULL,
  `Award_FTR_Price` DECIMAL(6,2) NOT NULL,
  PRIMARY KEY (`Auc_Res_ID`),
  UNIQUE INDEX `Auc_Res_ID_UNIQUE` (`Auc_Res_ID` ASC) VISIBLE)
COMMENT = 'Contains information of auction result records';

CREATE TABLE `proj`.`day_ahead_market` (
  `L_ID` INT NOT NULL,
  `Hour_Ending` INT NOT NULL,
  `Date` DATE NOT NULL,
  `LMP` DECIMAL(6,2) NOT NULL,
  `Energy_Comp` DECIMAL(6,2) NOT NULL,
  `Cog_Comp` DECIMAL(6,2) NOT NULL,
  `ML_Comp` DECIMAL(6,2) NOT NULL,
  PRIMARY KEY (`L_ID`, `Hour_Ending`))
COMMENT = 'Contains information about the day ahead market';

CREATE TABLE `proj`.`profit` (
  `P_ID` INT NOT NULL AUTO_INCREMENT,
  `C_ID` INT NOT NULL,
  `C_Name` VARCHAR(45) NOT NULL,
  `Date_Of_Profit` DATE NOT NULL,
  `Profit` DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (`P_ID`),
  UNIQUE INDEX `P_ID_UNIQUE` (`P_ID` ASC) VISIBLE)
COMMENT = 'Contains information calculated through cusomer name and a date';

