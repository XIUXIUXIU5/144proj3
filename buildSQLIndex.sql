CREATE TABLE ItemRegion  ENGINE=MyISAM SELECT ItemID, POINT(Latitude, Logitude) as LocationPoint FROM Item;
ALTER TABLE ItemRegion MODIFY LocationPoint point NOT NULL;
ALTER TABLE ItemRegion ADD SPATIAL INDEX(LocationPoint);
