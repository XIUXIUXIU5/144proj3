CREATE TABLE IF NOT EXISTS ItemRegion (ItemID varchar(40) NOT NULL,
						   LocationPoint POINT NOT NULL,
						   SPATIAL INDEX(LocationPoint),
						   PRIMARY KEY (ItemID, LocationPoint),
						   FOREIGN KEY (ItemID) REFERENCES Item(ItemID)
						   ) ENGINE=MyISAM;

INSERT INTO ItemRegion (ItemID, LocationPoint)
SELECT ItemID, POINT(Latitude, Logitude) 
FROM Item
WHERE Latitude IS NOT NULL AND Logitude IS NOT NULL;